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


def log_msg(*args):
    print(time.strftime('%H:%M:%S '), *args)


def hash_file(file_path):
    """ return hash of given file"""
    alg = hashlib.sha1()
    f = open(file_path, 'rb')
    buf = f.read(131072)
    while len(buf) > 0:
        alg.update(buf)
        buf = f.read(131072)
    f.close()
    return alg.hexdigest()


def check_file_info(file_infos, always_check_hash):
    removals = []
    additions = []
    for i in range(len(file_infos)):
        try:
            sr = os.stat(file_infos[i].path)
            stat_changed = file_infos[i].has_stat_changed(sr)
            if stat_changed or always_check_hash:
                hash_val = hash_file(file_infos[i].path)
                if stat_changed or (hash_val != file_infos[i].hash_val):
                    if hash_val != file_infos[i].hash_val:
                        log_msg('Hash changed: {}'.format(file_infos[i].path))
                    else:
                        log_msg('Mismatch file info: {}'.format(file_infos[i].path))
                    removals.append(i)
                    additions.append(FileInfo(file_infos[i].path, hash_val, sr))
        except OSError:
            log_msg('File deleted: {}'.format(file_infos[i].path))
            removals.append(i)
    removals.reverse()
    for i in removals:
        file_infos.pop(i)
    for v in additions:
        file_infos.append(v)


def check_file_info_exists(file_infos):
    removals = []
    for i in range(len(file_infos)):
        try:
            if not os.path.exists(file_infos[i].path):
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
        drive=drive[:-1]
    else:
        drive=''
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
                subprocess.call(['xdelta3.exe', '-e', '-s', full_backup, source, target_name])
            else:
                log_msg('Copying mail data.')
                shutil.copy2(source, target_name)
            stat_result = os.stat(target_name)
            file_count += 1
            file_size += stat_result.st_size
            win32api.SetFileAttributes(target_name, win32con.FILE_ATTRIBUTE_READONLY)
    return file_count, file_size


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
    log_msg('Loading dest hashes')
    populate_hash_dict(hash_targets, dest_hash_csv, always_hash_target)
    log_msg('Load source hashes')
    populate_name_dict(hash_sources, source_hash_csv, always_hash_source)
    new_bytes = 0
    log_msg('Executing backup')
    log_msg('Skip files: {}'.format(skip_files))
    new_files = 0
    linked_files = 0
    for source_dir in sources:
        for (dpath, dnames, fnames) in os.walk(source_dir):
            dest_dir = dest_path_from_source_path(backup_dir, dpath)
            os.makedirs(dest_dir, exist_ok=True)
            if dpath.count('\\') <= 4:
                log_msg('{}, total links: {}'.format(dpath, linked_files))
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
                try:
                    file_path = os.path.join(dpath, file_name)
                    if file_path not in skip_files:
                        attributes = win32api.GetFileAttributes(file_path)
                        # skip dehydrated files
                        # win32con does not define FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS 0x400000
                        #  or FILE_ATTRIBUTE_RECALL_ON_OPEN 0x40000
                        if (attributes & win32con.FILE_ATTRIBUTE_OFFLINE) == 0 and \
                                (attributes & 0x400000) == 0 and \
                                (attributes & 0x40000) == 0:
                            sr = os.stat(file_path)
                            info = None
                            if (not always_hash_source) and file_path in hash_sources:
                                info = hash_sources[file_path]
                            if info and not info.has_stat_changed(sr):
                                hash_val = info.hash_val
                            else:
                                log_msg('Hashing {}'.format(file_path))
                                hash_val = hash_file(file_path)
                                hash_sources[file_path] = FileInfo(file_path, hash_val, sr)
                            dest_path = dest_path_from_source_path(backup_dir, file_path)
                            use_copy = True
                            if hash_val in hash_targets:
                                # make link
                                try:
                                    os.link(hash_targets[hash_val].path, dest_path)
                                    linked_files += 1
                                    use_copy = False
                                except OSError:
                                    pass
                            if use_copy:
                                # copy new file
                                log_msg('new file {}'.format(file_path))
                                shutil.copy2(file_path, dest_path)
                                sr = os.stat(dest_path)
                                new_bytes += sr.st_size
                                hash_targets[hash_val] = FileInfo(dest_path, hash_val, sr)
                                new_files += 1
                                win32api.SetFileAttributes(dest_path, win32con.FILE_ATTRIBUTE_READONLY)
                        else:
                            log_msg('Skipping dehydrated file {}'.format(file_path))
                except OSError as error:
                    log_msg('Exception handling file {}, {}'.format(file_name, str(error)))
    write_file_infos(hash_targets, dest_hash_csv)
    write_file_infos(hash_sources, source_hash_csv)
    for hash_name in [dest_hash_csv, source_hash_csv]:
        hash_dest_path = dest_path_from_source_path(backup_dir, hash_name)
        # it's possible the file was already included in the backup. Don't copy over if so.
        if not os.path.exists(hash_dest_path):
            dir_path = os.path.split(hash_dest_path)[0]
            os.makedirs(dir_path, exist_ok=True)
            shutil.copy2(hash_name, hash_dest_path)
    log_msg('Total links: {:,}'.format(linked_files))
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


def delete_excess(dest_dir, dest_hashes_csv, max_backup_count):
    subdirs = []
    dir_list = os.scandir(dest_dir)
    for dir_entry in dir_list:
        if dir_entry.is_dir():
            subdirs.append(dir_entry.name)
    if len(subdirs) > max_backup_count:
        subdirs.sort()
        subdirs = subdirs[:len(subdirs) - max_backup_count]
        hash_dest = {}
        populate_name_dict(hash_dest, dest_hashes_csv, False)
        for subdir in subdirs:
            path_prefix = os.path.join(dest_dir, subdir)
            log_msg('Removing directory: {}', path_prefix)
            deletions = []
            additions = []
            for key, value in hash_dest.items():
                if key.startswith(path_prefix):
                    deletions.append(key)
                    links = get_hardlinks(key)
                    if links:
                        value.path = links[-1]
                        additions.append((value.path, value))
            for del_path in deletions:
                hash_dest.pop(del_path)
            for add_tuple in additions:
                hash_dest[add_tuple[0]] = add_tuple[1]
            # write the new hash list before attempting delete, in case of an error
            write_file_infos(hash_dest, dest_hashes_csv)
            shutil.rmtree(path_prefix, True)


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
          'subdirectories under the "dest:" directory) exceeds this number the oldest directories are removed. Any hash'
          ' targets in the directories to be removed are repointed to existing hardlinks or removed from the list if no'
          ' other hardlinks exist.')


def main():
    parser = argparse.ArgumentParser(description='Backup with hardlinks')
    parser.add_argument('config_file', help='Path to configuration yaml file')
    parser.add_argument('-help', help='Print detailed help information',
                        action='store_true')
    parser.add_argument('-date_override', help='Text to override date string. Used for script testing')
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
                        config['delta_files'].append(sources['delta_files'])
                    else:
                        config['delta_files'] = sources['delta_files']
                if 'latest_only_dirs' in sources:
                    if 'latest_only_dirs' in config:
                        config['latest_only_dirs'].append(sources['latest_only_dirs'])
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
            os.makedirs(backup_dir, exist_ok=True)
            new_files = 0
            new_bytes = 0
            skip_files = []
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
            counts = do_backup(backup_dir, config['sources'], config['dest_hashes'], config['source_hashes'],
                               latest_only_dirs, skip_files, always_hash_source, always_hash_target)
            if 'max_backup_count' in config:
                delete_excess(config['dest'], config['dest_hashes'], config['max_backup_count'])
            new_files += counts[0]
            new_bytes += counts[1]
            log_msg('New files: {:,}\nbytes: {:,}'.format(new_files, new_bytes))
        else:
            print('Config file missing required values. No backup.')
    else:
        print('No config file specified.')
        print_help()
    sys.exit(0)


if __name__ == "__main__":
    # execute only if run as a script
    main()
