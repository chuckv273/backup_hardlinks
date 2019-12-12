import hashlib
import csv
import os
from datetime import date
import shutil
import glob
import win32api
import win32con
import sys
import yaml
import argparse
import zipfile
import bsdiff4


class FileInfo:
    def __init__(self, path=None, hash=None, stat_info=None, csv_row=None):
        if csv_row:
            self.path = csv_row[0]
            self.hash = csv_row[1]
            self.size = int(csv_row[2])
            self.mtime_ns = int(csv_row[3])
            self.ctime_ns = int(csv_row[4])
        else:
            if path and hash and stat_info:
                self.path = path
                self.hash = hash
                self.size = stat_info.st_size
                self.mtime_ns = stat_info.st_mtime_ns
                self.ctime_ns = stat_info.st_ctime_ns
            else:
                raise ValueError


    def make_csv_row(self):
        return [self.path, self.hash, self.size, self.mtime_ns, self.ctime_ns]


    def has_stat_changed(self, stat_info):
        return (self.size != stat_info.st_size or
                self.mtime_ns != stat_info.st_mtime_ns or
                self.ctime_ns != stat_info.st_ctime_ns)


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
                if stat_changed or (hash_val != file_infos[i].hash):
                    print('Mismatch file info: {}'.format(file_infos[i].path))
                    removals.append(i)
                    additions.append(FileInfo(file_infos[i].path, hash_val, sr))
        except Exception:
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
        except Exception:
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
    except OSError:
        pass


def populate_hash_dict(hash_dict, file_name, check_hashes):
    file_infos = []
    populate_file_infos(file_infos, file_name)
    check_file_info(file_infos, check_hashes)
    for info in file_infos:
        hash_dict[info.hash] = info


def populate_name_dict(name_dict, file_name):
    file_infos = []
    populate_file_infos(file_infos, file_name)
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
    path = path[1:]
    return os.path.join(backup_dir, path)


def copy_mailbox(backup_dir):
    source = r'C:\Users\chuck\AppData\Local\Microsoft\Outlook\chuck@chuckvilla.org.ost'
    file_name = 'chuck@chuckvilla.org.ost'
    target_name = os.path.join(backup_dir, file_name)
    available = False
    file_size = 0
    file_count = 0
    # test if we can read the mail file. File is possible open and not available for backup
    try:
        test_file = open(source, 'rb')
        test_file.close()
        available = True
    except Exception:
        print('Mail file is not available. Skipping.')
        pass

    if available and len(backup_dir) > 4:
        print('Copying mail data.')
        # look for previous backups from which to make a delta
        # last two characters of backup_dir should be day. Replace them with '?'
        search_path = backup_dir[:-2] + '??'
        bdirs = glob.glob(search_path)
        full_backup = None
        for bdir in bdirs:
            check_path = os.path.join(bdir, file_name)
            if os.path.exists(check_path):
                full_backup = check_path
                break
        if full_backup:
            print('Full mail backup found: {}. Generating delta.'.format(full_backup))
            patch_name = target_name + '.bsdiff.patch'
            bsdiff4.file_diff(full_backup, source, patch_name)
            stat_result = os.stat(patch_name)
            file_count = 1
            file_size = stat_result.st_size
            win32api.SetFileAttributes(patch_name, win32con.FILE_ATTRIBUTE_READONLY)
        else:
            shutil.copy2(source, target_name)
            stat_result = os.stat(target_name)
            file_count = 1
            file_size = stat_result.st_size
            win32api.SetFileAttributes(target_name, win32con.FILE_ATTRIBUTE_READONLY)
    return file_count, file_size


def copy_c_backup_zip(backup_dir):
    source_dir = r'C:\Backup'
    file_name = 'backup.zip'
    target_name = os.path.join(backup_dir, file_name)
    available = False
    file_size = 0
    file_count = 0
    # test if we can read the mail file. File is possible open and not available for backup
    zip = zipfile.ZipFile(target_name, "w", zipfile.ZIP_DEFLATED)
    for file in os.listdir(source_dir):
        source_file = os.path.join(source_dir, file)
        if os.path.isfile(source_file):
            zip.write(source_file, arcname=file)
    zip.close()
    sr = os.stat(target_name)
    return 1, sr.st_size


def do_backup(backup_dir, sources, dest_hash_csv, source_hash_csv, check_dest_hashes):
    """
    :param backup_dir: str: destination directory for backup
    :param sources: list of source paths. All sub dirs are included
    :param dest_hash_csv: csv file with hashes on destination volume
    :param source_hash_csv: csv file with hashes on source volume
    :param check_dest_hashes: bool: if true, rehashes files on dest volume to verify hashes
    :return:
    """
    hash_targets = {}
    hash_sources = {}
    print('Loading dest hashes')
    populate_hash_dict(hash_targets, dest_hash_csv, check_dest_hashes)
    print('Load source hashes')
    populate_name_dict(hash_sources, source_hash_csv)
    new_bytes = 0
    print('Executing backup')
    new_files = 0
    for source_dir in sources:
        for (dpath, dnames, fnames) in os.walk(source_dir):
            dest_dir = dest_path_from_source_path(backup_dir, dpath)
            os.makedirs(dest_dir, exist_ok=True)
            if dpath.count('\\') <= 4:
                print(dpath)
            # Only take latest file from Parity server backups
            if dpath == 'C:\\Users\\jen-chuck\\OneDrive\\Parity':
                lastest_time = 0
                latest_name = None
                for file_name in fnames:
                    sr = os.stat(os.path.join(dpath, file_name))
                    if sr.st_mtime_ns > lastest_time:
                        lastest_time = sr.st_mtime_ns
                        latest_name = file_name
                fnames = [latest_name]
            for file_name in fnames:
                try:
                    file_path = os.path.join(dpath, file_name)
                    attributes = win32api.GetFileAttributes(file_path)
                    # skip dehydrated files
                    # win32con does not define FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS 0x400000
                    #  or FILE_ATTRIBUTE_RECALL_ON_OPEN 0x40000
                    if (attributes & win32con.FILE_ATTRIBUTE_OFFLINE) == 0 and \
                        (attributes & 0x400000) == 0 and \
                        (attributes & 0x40000) == 0 :
                        sr = os.stat(file_path)
                        if file_path in hash_sources:
                            info = hash_sources[file_path]
                        else:
                            info = None
                        if info and not info.has_stat_changed(sr):
                            hash_val = info.hash
                        else:
                            print('Hashing {}'.format(file_path))
                            hash_val = hash_file(file_path)
                            hash_sources[file_path] = FileInfo(file_path, hash_val, sr)
                        dest_path = dest_path_from_source_path(backup_dir, file_path)
                        use_copy = True
                        if hash_val in hash_targets:
                            # make link
                            try:
                                os.link(hash_targets[hash_val].path, dest_path)
                                use_copy = False
                            except OSError:
                                pass
                        if use_copy:
                            # copy new file
                            print('new file {}'.format(file_path))
                            shutil.copy2(file_path, dest_path)
                            sr = os.stat(dest_path)
                            new_bytes += sr.st_size
                            hash_targets[hash_val] = FileInfo(dest_path, hash_val, sr)
                            new_files += 1
                            win32api.SetFileAttributes(dest_path, win32con.FILE_ATTRIBUTE_READONLY)
                    else:
                        print('Skipping dehydrated file {}'.format(file_path))
                except OSError:
                    print('Exception handling file {}'.format(file_path))
    write_file_infos(hash_targets, dest_hash_csv)
    write_file_infos(hash_sources, source_hash_csv)
    for hash_name in [dest_hash_csv, source_hash_csv]:
        hash_dest_path = dest_path_from_source_path(backup_dir, hash_name)
        # it's possible the file was already included in the backup. Don't copy over if so.
        if not os.path.exists(hash_dest_path):
            dir_path = os.path.split(hash_dest_path)[0]
            os.makedirs(dir_path, exist_ok=True)
            shutil.copy2(hash_name, hash_dest_path)
    return new_files, new_bytes


parser = argparse.ArgumentParser(description='Backup with hardlinks')
parser.add_argument('config_file', help='Path to configuration yaml file')
parser.add_argument('--check-hashes', help='Verify stored destination hashes. Can add a lot of time to the backup.',
                    action='store_true')
args = parser.parse_args()

if args.config_file:
    config = {}
    with open(args.config_file, 'r') as stream:
        config = yaml.safe_load(stream)
    if 'sources_file' in config:
        with open(config['sources_file'], 'r') as stream:
            sources = yaml.safe_load(stream)
            if 'sources' in sources:
                config['sources'] = sources['sources']
    if 'dest' in config and 'sources' in config and 'dest_hashes' in config and 'source_hashes' in config:
        use_date = True
        if 'use_date' in config and not config['use_date']:
            use_date = False
        if use_date:
            backup_dir = os.path.join(config['dest'], date.today().strftime('%Y-%m-%d'))
        else:
            backup_dir = config['dest']
        copy_mail = True
        if 'copy_mail' in config and not config['copy_mail']:
            copy_mail = False
        copy_c_backup = True
        if 'copy_c_backup' in config and not config['copy_c_backup']:
            copy_c_backup = False
        os.makedirs(backup_dir, exist_ok=True)
        new_files = 0
        new_bytes = 0
        if copy_mail:
            try:
                counts = copy_mailbox(backup_dir)
                new_files += counts[0]
                new_bytes += counts[1]
            except OSError:
                print('Failure copying mail data. File likely open.')
        if copy_c_backup:
            try:
                counts = copy_c_backup_zip(backup_dir)
                new_files += counts[0]
                new_bytes += counts[1]
            except OSError:
                print('Failure copying backup directory.')
        counts = do_backup(backup_dir, config['sources'], config['dest_hashes'], config['source_hashes'],
                           args.check_hashes)
        new_files += counts[0]
        new_bytes += counts[1]
        print('New files: {:,}\nbytes: {:,}'.format(new_files, new_bytes))
    else:
        print('Config file missing required values. No backup.')
else:
    print('No config file specified.')
sys.exit(0)
