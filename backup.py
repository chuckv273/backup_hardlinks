import hashlib
import csv
import os
from datetime import date
import shutil
import glob
import win32api
import win32con
import win32file
import sys
import yaml
import argparse
import subprocess
import time
import threading
import queue
import typing
import pywintypes
import zipfile


class FileInfo:
    def __init__(self, path=None, hash_val=None, stat_info=None, csv_row=None):
        if csv_row:
            self.path = csv_row[0]
            self.hash_val = csv_row[1]
            self.size = int(csv_row[2])
            self.mtime_ns = int(csv_row[3])
            self.ctime_ns = int(csv_row[4])
        else:
            if path and hash_val and stat_info:
                self.path = path
                self.hash_val = hash_val
                self.size = stat_info.st_size
                self.mtime_ns = stat_info.st_mtime_ns
                self.ctime_ns = stat_info.st_ctime_ns
            else:
                raise ValueError

    def make_csv_row(self):
        return [self.path, self.hash_val, self.size, self.mtime_ns, self.ctime_ns]

    def has_stat_changed(self, stat_info):
        return (self.size != stat_info.st_size or
                self.mtime_ns != stat_info.st_mtime_ns or
                self.ctime_ns != stat_info.st_ctime_ns)


log_lock = threading.Lock()
def log_msg(*args):
    with log_lock:
        print(time.strftime('%H:%M:%S '), threading.get_ident(), ' ', *args, flush=True)


def hash_file(file_path):
    """ return hash of given file"""
    alg = hashlib.sha1()
    f = open(file_path, 'rb')
    size = 1024 * 1024
    buf = f.read(size)
    while len(buf) > 0:
        alg.update(buf)
        buf = f.read(size)
    f.close()
    return alg.hexdigest()


def run_threaded(func, args):
    thread_count = 16
    threads = []
    for i in range(thread_count):
        thread = (threading.Thread(target=func, args=args))
        threads.append(thread)
        thread.start()
    queue_param = None
    # Multiple parameters for a tuple
    if isinstance(args, tuple) and len(args) > 0 and isinstance(args[0], queue.Queue):
        while not args[0].empty() and threads[0].is_alive():
            log_msg('Run threaded: queue size {}'.format(args[0].qsize()))
            threads[0].join(300)
    for thread in threads:
        thread.join()


def check_file_info_worker(work_q: queue.Queue, file_infos: typing.List[FileInfo], removal_q: queue.Queue,
                           addition_q: queue.Queue, always_check_hash: bool):
    while True:
        try:
            index = work_q.get_nowait()
        except queue.Empty:
            return
        try:
            sr = os.stat(file_infos[index].path)
            stat_changed = file_infos[index].has_stat_changed(sr)
            if stat_changed or always_check_hash:
                hash_val = hash_file(file_infos[index].path)
                if stat_changed or (hash_val != file_infos[index].hash_val):
                    if hash_val != file_infos[index].hash_val:
                        log_msg('Hash changed: {}'.format(file_infos[index].path))
                    else:
                        log_msg('Mismatch file info: {}'.format(file_infos[index].path))
                    removal_q.put(index)
                    addition_q.put(FileInfo(file_infos[index].path, hash_val, sr))
        except OSError:
            log_msg('File deleted: {}'.format(file_infos[index].path))
            removal_q.put(index)
        work_q.task_done()


def check_file_info(file_infos, always_check_hash):
    work_queue = queue.Queue()
    removal_q = queue.Queue()
    addition_q = queue.Queue()
    for i in range(len(file_infos)):
        work_queue.put(i)
    log_msg('check_file_info, work size: {}'.format(len(file_infos)))
    run_threaded(check_file_info_worker, (work_queue, file_infos, removal_q, addition_q, always_check_hash))
    removals = []
    while not removal_q.empty():
        removals.append(removal_q.get_nowait())
    removals.sort(reverse = True)
    for i in removals:
        file_infos.pop(i)
    while not addition_q.empty():
        file_infos.append(addition_q.get_nowait())


def check_file_info_exists(file_infos):
    removals = []
    for i in range(len(file_infos)):
        try:
            if not os.path.exists(file_infos[i].path):
                log_msg('File deleted: {}'.format(file_infos[i].path))
                removals.append(i)
        except OSError:
            removals.append(i)
    removals.reverse()
    for i in removals:
        file_infos.pop(i)


def populate_file_infos(file_infos, file_name):
    try:
        csvfile = open(file_name, 'r', newline='')
        reader = csv.reader(csvfile)
        for row in reader:
            file_infos.append(FileInfo(csv_row=row))
        csvfile.close()
    except OSError as error:
        log_msg('Error reading csv: {}, {}', format(file_name, str(error)))


def populate_hash_dict(hash_dict, file_name, check_hashes):
    file_infos = []
    populate_file_infos(file_infos, file_name)
    check_file_info(file_infos, check_hashes)
    for info in file_infos:
        hash_dict[info.hash_val] = info


def populate_name_dict(name_dict, file_name, check_existence):
    file_infos = []
    populate_file_infos(file_infos, file_name)
    if check_existence:
        check_file_info_exists(file_infos)
    for info in file_infos:
        name_dict[info.path] = info


def write_file_infos(info_dict, file_name):
    csvfile = open(file_name, 'w', newline='')
    writer = csv.writer(csvfile)
    for info in info_dict.values():
        writer.writerow(info.make_csv_row())
    csvfile.close()


def dest_path_from_source_path(backup_dir, source_path):
    drive, path = os.path.splitdrive(source_path)
    # Trim trailing ':' from drive
    if len(drive) > 1:
        drive = drive[:-1]
    else:
        drive = ''
    path = path[1:]
    # join ignores empty elements, so it's OK if drive is empty
    return os.path.join(backup_dir, drive, path)


def generate_delta_files(backup_dir, delta_files):
    file_size = 0
    file_count = 0
    for source in delta_files:
        target_name = dest_path_from_source_path(backup_dir, source)
        # test if we can read the source file. File is possibly open and not available for backup
        try:
            test_file = open(source, 'rb')
            test_file.close()
            available = True
        except OSError:
            log_msg('File {} is not available. Skipping.'.format(source))
            available = False

        if available and len(backup_dir) > 4:
            os.makedirs(os.path.split(target_name)[0], exist_ok=True)
            # look for previous backups from which to make a delta
            # last two characters of backup_dir should be day. Replace them with '?'
            search_path = backup_dir[:-2] + '??'
            bdirs = glob.glob(search_path)
            full_backup = None
            for bdir in bdirs:
                check_path = dest_path_from_source_path(bdir, source)
                if os.path.exists(check_path):
                    # only use non-empty files as a source
                    # this avoids using an empty file as the base if there was an error copying the file
                    sr = os.stat(check_path)
                    if sr.st_size > 0:
                        full_backup = check_path
                        break
            if full_backup:
                log_msg('Full backup found: {}. Generating delta.'.format(full_backup))
                target_name = target_name + '.patch'
                log_msg('Calling xdelta3 full={}, source={}, target={}'.format(full_backup, source, target_name))
                subprocess.call(['xdelta3.exe', '-e', '-B', '1000000000', '-W', '16777216', '-s', full_backup, source,
                                 target_name])
            else:
                log_msg('Copying source: {}.'.format(source))
                shutil.copy2(source, target_name)
            stat_result = os.stat(target_name)
            file_count += 1
            file_size += stat_result.st_size
            win32api.SetFileAttributes(target_name, win32con.FILE_ATTRIBUTE_READONLY)
    return file_count, file_size


def generate_compressed_files(backup_dir, source_files):
    file_size = 0
    file_count = 0
    for source in source_files:
        target_name = dest_path_from_source_path(backup_dir, source + '.zip')
        # test if we can read the source file. File is possibly open and not available for backup
        try:
            test_file = open(source, 'rb')
            test_file.close()
            available = True
        except OSError:
            log_msg('File {} is not available. Skipping.'.format(source))
            available = False

        if available:
            os.makedirs(os.path.split(target_name)[0], exist_ok=True)
            log_msg('compressing file {}.'.format(source))
            zip = zipfile.ZipFile(target_name, "w", zipfile.ZIP_DEFLATED)
            zip.write(source)
            zip.close()
            stat_result = os.stat(target_name)
            file_count += 1
            file_size += stat_result.st_size
            win32api.SetFileAttributes(target_name, win32con.FILE_ATTRIBUTE_READONLY)
    return file_count, file_size


def backup_worker(source_queue: queue.Queue, backup_dir: str, hash_sources, hash_source_lock: threading.Lock,
                  always_hash_source: bool, hash_targets, hash_target_lock: threading.Lock, results_queue: queue.Queue):
    linked_files = 0
    linked_size = 0
    new_bytes = 0
    new_files = 0
    while True:
        try:
            file_path = source_queue.get_nowait()
        except queue.Empty:
            results_queue.put( (linked_files, linked_size, new_bytes, new_files) )
            return
        try:
            attributes = win32api.GetFileAttributes(file_path)
            # skip dehydrated files
            # win32con does not define FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS 0x400000
            #  or FILE_ATTRIBUTE_RECALL_ON_OPEN 0x40000
            if (attributes & win32con.FILE_ATTRIBUTE_OFFLINE) == 0 and \
                    (attributes & 0x400000) == 0 and \
                    (attributes & 0x40000) == 0:
                sr = os.stat(file_path)
                info = None
                if not always_hash_source:
                    with hash_source_lock:
                        if file_path in hash_sources:
                            info = hash_sources[file_path]
                if info and not info.has_stat_changed(sr):
                    hash_val = info.hash_val
                else:
                    if not always_hash_source:
                        log_msg('Hashing {}'.format(file_path))
                    hash_val = hash_file(file_path)
                    hash_sources[file_path] = FileInfo(file_path, hash_val, sr)
                dest_path = dest_path_from_source_path(backup_dir, file_path)
                use_copy = True
                target_val = None
                with hash_target_lock:
                    if hash_val in hash_targets:
                        target_val = hash_targets[hash_val]
                if target_val:
                    # make link
                    try:
                        os.link(target_val.path, dest_path)
                        linked_files += 1
                        linked_size += sr.st_size
                        use_copy = False
                    except OSError:
                        pass
                if use_copy:
                    # copy new file
                    log_msg('new file {}'.format(file_path))
                    shutil.copy2(file_path, dest_path)
                    win32api.SetFileAttributes(dest_path, win32con.FILE_ATTRIBUTE_READONLY)
                    sr = os.stat(dest_path)
                    new_bytes += sr.st_size
                    new_files += 1
                    with hash_target_lock:
                        hash_targets[hash_val] = FileInfo(dest_path, hash_val, sr)
            else:
                log_msg('Skipping dehydrated file {}'.format(file_path))

        except OSError as error:
            log_msg('Exception handling file {}, {}'.format(file_path, str(error)))
        source_queue.task_done()



def do_backup(backup_dir, sources, dest_hash_csv, source_hash_csv, latest_only_dirs, skip_files, always_hash_source,
              always_hash_target):
    """
    :param backup_dir: str: destination directory for backup
    :param sources: list of source paths. All sub dirs are included
    :param dest_hash_csv: csv file with hashes on destination volume
    :param source_hash_csv: csv file with hashes on source volume
    :param latest_only_dirs: list of directories from which only the single latest file is saved
    :param skip_files: list of full paths that should be skipped (e.g. already captured via binary delta)
    :param always_hash_source: bool: if true, always hashes source file, withot checking size or timestamps
    :param always_hash_target: bool: if true, rehashes files on dest volume to verify hashes
    :return:
    """
    hash_targets = {}
    hash_sources = {}
    log_msg('Loading dest hashes. Always hash target: {}'.format(always_hash_target))
    populate_hash_dict(hash_targets, dest_hash_csv, always_hash_target)
    log_msg('Load source hashes. Always hash source: {}'.format(always_hash_source))
    populate_name_dict(hash_sources, source_hash_csv, always_hash_source)
    new_bytes = 0
    log_msg('Executing backup')
    log_msg('Skip files: {}'.format(skip_files))
    new_files = 0
    linked_files = 0
    linked_size = 0
    source_queue = queue.Queue()
    for source_dir in sources:
        for (dpath, dnames, fnames) in os.walk(source_dir):
            dest_dir = dest_path_from_source_path(backup_dir, dpath)
            os.makedirs(dest_dir, exist_ok=True)
            if dpath in latest_only_dirs:
                lastest_time = 0
                file_selected = []
                for file_name in fnames:
                    sr = os.stat(os.path.join(dpath, file_name))
                    if sr.st_mtime_ns > lastest_time:
                        lastest_time = sr.st_mtime_ns
                        file_selected = [file_name]
                fnames = file_selected
            for file_name in fnames:
                file_path = os.path.join(dpath, file_name)
                if file_path not in skip_files:
                    source_queue.put(file_path)
    source_lock = threading.Lock()
    target_lock = threading.Lock()
    results = queue.Queue()
    log_msg('do_backup, work size: {}'.format(source_queue.qsize()))
    run_threaded(backup_worker, (source_queue, backup_dir, hash_sources, source_lock, always_hash_source, hash_targets,
                                 target_lock, results))
    while not results.empty():
        lf, ls, ns, nf = results.get_nowait()
        linked_files += lf
        linked_size += ls
        new_bytes += ns
        new_files += nf
    write_file_infos(hash_targets, dest_hash_csv)
    write_file_infos(hash_sources, source_hash_csv)
    for hash_name in [dest_hash_csv, source_hash_csv]:
        hash_dest_path = dest_path_from_source_path(backup_dir, hash_name)
        # it's possible the file was already included in the backup. Don't copy over if so.
        if not os.path.exists(hash_dest_path):
            dir_path = os.path.split(hash_dest_path)[0]
            os.makedirs(dir_path, exist_ok=True)
            shutil.copy2(hash_name, hash_dest_path)
    log_msg('Link count: {:,}, linked size: {:,}'.format(linked_files, linked_size))
    log_msg('Total files: {:,}, total size: {:,}'.format(linked_files+new_files, linked_size+new_bytes))
    return new_files, new_bytes


# returns a list of hardlinks for the given path, excluding the original path
def get_hardlinks(path):
    drive, no_drive_path = os.path.splitdrive(path)
    hardlinks = []
    temp_names = win32file.FindFileNames(path)
    # the response from win32file.FindFileNames needs some fixup
    # We need to add the drive letter and remove the trailing NUL
    for t_name in temp_names:
        fixed_name = t_name[:-1]
        # don't include the original path
        if fixed_name != no_drive_path:
            hardlinks.append(drive + fixed_name)
    return hardlinks


def remove_tree_worker(delete_queue, root):
    while True:
        try:
            file_path = delete_queue.get_nowait()
        except queue.Empty:
            return
        exterior_path = None
        paths_to_delete = [file_path]
        hard_links = get_hardlinks(file_path)
        for link in hard_links:
            if link.startswith(root):
                paths_to_delete.append(link)
            else:
                exterior_path = link
        if not exterior_path:
            log_msg('Deleting file: {}'.format(file_path))
        try:
            win32api.SetFileAttributes(file_path, win32con.FILE_ATTRIBUTE_NORMAL)
            for path in paths_to_delete:
                os.remove(path)
        except OSError as error:
            log_msg('Exception removing file {}, {}'.format(file_path, str(error)))
        except pywintypes.error as pyw_error:
            log_msg('Exception removing file {}, {}'.format(file_path, str(pyw_error)))
        if len(hard_links) > 0:
            try:
                win32api.SetFileAttributes(hard_links[-1], win32con.FILE_ATTRIBUTE_READONLY)
            except OSError:
                pass
            except pywintypes.error:
                pass
        delete_queue.task_done()


def walk_tree_worker(walk_queue, delete_queue, file_ids, id_lock):
    while True:
        try:
            file_path = walk_queue.get_nowait()
        except queue.Empty:
            return
        s_info = os.stat(file_path)
        with id_lock:
            if s_info.st_ino not in file_ids:
                file_ids.add(s_info.st_ino)
                delete_queue.put(file_path)
        walk_queue.task_done()


def remove_tree(path):
    log_msg('Remove tree: {}'.format(path))
    delete_queue = queue.Queue()
    file_ids = set()
    walk_queue = queue.Queue()
    id_lock = threading.Lock()
    log_msg('Generating deletion list.')
    for (dpath, dnames, fnames) in os.walk(path):
        for file_name in fnames:
            file_path = os.path.join(dpath, file_name)
            walk_queue.put(file_path)
    log_msg('Walk list size: {}'.format(walk_queue.qsize()))
    run_threaded(walk_tree_worker, (walk_queue, delete_queue, file_ids, id_lock))
    log_msg('Delete list size {}'.format(delete_queue.qsize()))
    run_threaded(remove_tree_worker, (delete_queue, path))
    try:
        shutil.rmtree(path, True)
    except OSError as error:
        log_msg('Exception removing tree {}, {}'.format(path, str(error)))


def delete_excess(dest_dir, dest_hashes_csv, max_backup_count):
    subdirs = []
    dir_list = os.scandir(dest_dir)
    for dir_entry in dir_list:
        if dir_entry.is_dir():
            subdirs.append(dir_entry.name)
    log_msg('Checking excess. Max count: {}, directory count: {}'.format(max_backup_count, len(subdirs)))
    if len(subdirs) > max_backup_count:
        subdirs.sort()
        subdirs = subdirs[:len(subdirs) - max_backup_count]
        hash_dest = {}
        populate_name_dict(hash_dest, dest_hashes_csv, False)
        for subdir in subdirs:
            path_prefix = os.path.join(dest_dir, subdir)
            log_msg('Removing directory: {}'.format(path_prefix))
            deletions = []
            additions = []
            for key, value in hash_dest.items():
                if key.startswith(path_prefix):
                    deletions.append(key)
                    links = get_hardlinks(key)
                    if links:
                        for link in reversed(links):
                            if not link.startswith(path_prefix):
                                value.path = link
                                additions.append((value.path, value))
                                break
            for del_path in deletions:
                hash_dest.pop(del_path)
            for add_tuple in additions:
                hash_dest[add_tuple[0]] = add_tuple[1]
            # write the new hash list before attempting delete, in case of an error
            write_file_infos(hash_dest, dest_hashes_csv)
            remove_tree(path_prefix)


def print_help():
    print('backup.py - Backup with hardlinks')
    print('python backup.py config_file [-help]')
    print('This script maintains a catalog of hashes on the backup source and target. When creating a new backup file '
          'this allows us to hardlink the new files rather than copying a new set of bits. The first backup set '
          'consumes the full size, but later sets only use space for new or changed content. Unchanged files only '
          'require a hardlink\n')
    print('Options are stored in a yaml config file. All path comparisons are case sensitive. You must write any path '
          'exactly as the OS presents it.')
    print('sources: Required. A yaml string list of source directories. Each directory is fully traversed during the '
          'backup.')
    print('dest: Required. The path to the backup destination. Backups become subdirectories as YYYY-MM-DD. ')
    print('source_hashes: Required. A csv file to load and store source file info. Each source file has hash, size, and'
          ' timestamps. Size and timestamps are used to avoid rehashing. Can be non-existent at first, the script will '
          'generate it as needed. It should never be edited, the script will read and write it as needed.')
    print('dest_hashes: Required. A csv file to load and store destination file info. Each unique hash in the target '
          'area is tracked with path, hash, size, and timestamps. When a source file matches a target hash, a hardlink '
          'is created instead of a full copy. Size and timestamps are used to check for changes at start. Can be '
          'non-existent at first, the script will generate it as needed. It should never be edited, the script will '
          'read and write it as needed.')
    print('sources_file: Optional. Pull the sources list from a separate yaml file. This will add any entries to '
          'the local "sources:", "delta_files:", and "latest_only_dirs:". Useful when multiple backup sets need the '
          'same source list.')
    print('delta_files: Optional. A yaml string list of files to generate a binary delta of. Very useful for large mail'
          ' store files. At first, a full copy of the file is made. On subsequent backups, if a full version is found '
          'in the earlier backups then a binary delta from the earlier full version is stored. Given the YYYY-MM-DD '
          'format, the routine looks for YYYY-MM-??, basically any full copy within the current month. This option '
          'requires the utility xdelta3.exe to be on the path. This option is incompatible with "use_date: false"')
    print('use_date: Optional, default true. true or false. Sets whether a date encoded subdirectory should be created '
          'under the dest: directory. Useful if copying a set of already dated archives to a new destination')
    print('always_hash_source: Optional, default false. If true, source files are hashed every time. If false, size and'
          ' timestamps are used to determine if a source file has changed.')
    print('always_hash_target: Optional, default false. If true, at start hash targets in the dest directory are '
          'rehashed to confirm our current hash information is correct. Only the unique hash targets (not all files) '
          'are hashed. If false, size and timestamps are used to determine if a target file has changed.')
    print('latest_only_dirs: Optional. If any of these directories are traversed, only the single latest file is '
          'included. All other files are skipped. Useful for log or backup directories for other software.')
    print('max_backup_count: Optional. Numeric. If set, when the backup count (as counted by the number of '
          'subdirectories under the "dest:" directory) exceeds this number the oldest directories are removed. This '
          'option requires the backup directories lexicographically sort in date order. Timestamps are not used. Any '
          'hash targets in the directories to be removed are repointed to existing hardlinks or removed from the list '
          'if no other hardlinks exist.')
    print('thread_count')


def main():
    parser = argparse.ArgumentParser(description='Backup with hardlinks')
    parser.add_argument('config_file', help='Path to configuration yaml file')
    parser.add_argument('-help', help='Print detailed help information',
                        action='store_true')
    parser.add_argument('-date_override', help='Text to override date string. Used for script testing')
    parser.add_argument('-no_backup', action='store_true', help='Skip backup, do delete check. Used for testing')
    args = parser.parse_args()

    if args.help:
        print_help()
    elif args.config_file:
        with open(args.config_file, 'r') as stream:
            config = yaml.safe_load(stream)
        if 'sources_file' in config:
            print('Using sources file: {}'.format(config['sources_file']))
            with open(config['sources_file'], 'r') as stream:
                sources = yaml.safe_load(stream)
                if 'sources' in sources:
                    if 'sources' in config:
                        config['sources'].append(sources['sources'])
                    else:
                        config['sources'] = sources['sources']
                if 'delta_files' in sources:
                    if 'delta_files' in config:
                        config['delta_files'].extend(sources['delta_files'])
                    else:
                        config['delta_files'] = sources['delta_files']
                if 'compressed_files' in sources:
                    if 'compressed_files' in config:
                        config['compressed_files'].extend(sources['compressed_files'])
                    else:
                        config['compressed_files'] = sources['compressed_files']
                if 'latest_only_dirs' in sources:
                    if 'latest_only_dirs' in config:
                        config['latest_only_dirs'].extend(sources['latest_only_dirs'])
                    else:
                        config['latest_only_dirs'] = sources['latest_only_dirs']
        if 'dest' in config and 'sources' in config and 'dest_hashes' in config and 'source_hashes' in config:
            use_date = True
            if 'use_date' in config and not config['use_date']:
                use_date = False
            if use_date:
                if args.date_override:
                    date_string = args.date_override
                else:
                    date_string = date.today().strftime('%Y-%m-%d')
                backup_dir = os.path.join(config['dest'], date_string)
            else:
                backup_dir = config['dest']
            if (not use_date) and 'delta_files' in config and len(config['delta_files']) > 0:
                print('Setting use_date: false and having delta_files is incompatible. Exiting.')
                sys.exit(1)
            always_hash_source = False
            if 'always_hash_source' in config and config['always_hash_source']:
                always_hash_source = True
            always_hash_target = False
            if 'always_hash_target' in config and config['always_hash_target']:
                always_hash_target = True
            latest_only_dirs = []
            if 'latest_only_dirs' in config:
                latest_only_dirs = config['latest_only_dirs']
#            if 'thread_count' in config:
#                thread_count = config['thread_count']
            print('dest: {}'.format(config['dest']))
            print('sources: {}'.format(config['sources']))
            print('dest_hashes: {}'.format(config['dest_hashes']))
            print('source_hashes: {}'.format(config['source_hashes']))
            print('use_date: {}'.format(use_date))
            print('always_hash_source: {}'.format(always_hash_source))
            print('always_hash_target: {}'.format(always_hash_target))
            print('latest_only_dirs: {}'.format(latest_only_dirs))
            print('backup directory: {}'.format(backup_dir))
            if 'max_backup_count' in config:
                print('max_backup_count: {}'.format(config['max_backup_count']))
            else:
                print('max_backup_count: not set')
            print('no_backup: {}'.format(args.no_backup))
#            print('thread_count: {}'.format(thread_count))
            os.makedirs(backup_dir, exist_ok=True)
            new_files = 0
            new_bytes = 0
            skip_files = []
            counts = (0, 0)
            if not args.no_backup:
                if 'delta_files' in config:
                    log_msg('delta_files: {}'.format(config['delta_files']))
                    # since we made a delta of the file, make sure we skip it during the actual backup
                    # it's possible the delta file is included in the traversal of the main backup
                    skip_files.extend(config['delta_files'])
                    try:
                        counts = generate_delta_files(backup_dir, config['delta_files'])
                        new_files += counts[0]
                        new_bytes += counts[1]
                    except OSError as error:
                        log_msg('Failure generating delta files. {}'.format(str(error)))
                if 'compressed_files' in config:
                    log_msg('compressed_files: {}'.format(config['compressed_files']))
                    # since we made a zip of the file, make sure we skip it during the actual backup
                    # it's possible the zip file is included in the traversal of the main backup
                    skip_files.extend(config['compressed_files'])
                    try:
                        counts = generate_compressed_files(backup_dir, config['compressed_files'])
                        new_files += counts[0]
                        new_bytes += counts[1]
                    except OSError as error:
                        log_msg('Failure generating compressed files. {}'.format(str(error)))
                counts = do_backup(backup_dir, config['sources'], config['dest_hashes'], config['source_hashes'],
                                   latest_only_dirs, skip_files, always_hash_source, always_hash_target)
            if 'max_backup_count' in config:
                delete_excess(config['dest'], config['dest_hashes'], config['max_backup_count'])
            new_files += counts[0]
            new_bytes += counts[1]
            log_msg('New files: {:,}, bytes: {:,}'.format(new_files, new_bytes))
        else:
            print('Config file missing required values. No backup.')
    else:
        print('No config file specified.')
        print_help()
    sys.exit(0)


if __name__ == "__main__":
    # execute only if run as a script
    main()
