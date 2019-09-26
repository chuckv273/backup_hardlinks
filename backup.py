import hashlib
import csv
import os
from datetime import date
import shutil
import glob
import subprocess
import win32api
import win32con
import sys
import argparse


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
            sr = os.stat(file_infos[i][0])
            file_changed = ((sr.st_size != int(file_infos[i][2])) or (sr.st_mtime_ns != int(file_infos[i][3])) or \
                    (sr.st_ctime_ns != int(file_infos[i][4])))
            if file_changed or always_check_hash:
                hash_val = hash_file(file_infos[i][0])
                if file_changed or (hash_val != file_infos[i][1]):
                    print('Mismatch file info: {}'.format(file_infos[i][0]))
                    removals.append(i)
                    additions.append([file_infos[i][0], hash_val, sr.st_size, sr.st_mtime_ns, sr.st_ctime_ns])
        except Exception:
            removals.append(i)
    removals.reverse()
    for i in removals:
        file_infos.pop(i)
    for v in additions:
        file_infos.append(v)


def populate_file_infos(file_infos, file_name, check_hashes):
    try:
        csvfile = open(file_name, 'r', newline='')
        reader = csv.reader(csvfile)
        for row in reader:
            file_infos.append(row)
        csvfile.close()
    except Exception:
        pass
    check_file_info(file_infos, check_hashes)


def populate_hash_dict(hash_dict, file_name, check_hashes):
    file_infos = []
    populate_file_infos(file_infos, file_name, check_hashes)
    for info in file_infos:
        hash_dict[info[1]] = info


def populate_name_dict(name_dict, file_name):
    file_infos = []
    populate_file_infos(file_infos, file_name, False)
    for info in file_infos:
        name_dict[info[0]] = info


def populate_g_list():
    csvfile = open('C:\\Backup\\g-hash.csv', 'w', newline='')
    writer = csv.writer(csvfile)
    for (dpath, dnames, fnames) in os.walk('G:\\Backups'):
        print(dpath)
        for file_name in fnames:
            file_path = os.path.join(dpath, file_name)
            sr = os.stat(file_path)
            hash_val = hash_file(file_path)
            writer.writerow([file_path, hash_val, sr.st_size, sr.st_mtime_ns, sr.st_ctime_ns])
    csvfile.close()


def write_file_infos(info_dict, file_name):
    csvfile = open(file_name, 'w', newline='')
    writer = csv.writer(csvfile)
    for info in info_dict.values():
        writer.writerow(info)
    csvfile.close()


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
        # last two characters of backup_dir shold be day. Replace them with '?'
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
            patch_name = target_name + '.patch'
            subprocess.call(['xdelta3.exe', '-e', '-s', full_backup, source, patch_name])
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


def do_backup(backup_dir, check_g_hashes):
    hash_targets = {}
    hash_sources = {}
    print('Checking G:\ hashes')
    populate_hash_dict(hash_targets, 'C:\\Backup\\g-hash.csv', check_g_hashes)
    print('Checking C:\ hashes')
    populate_name_dict(hash_sources, 'C:\\Backup\\c-hash.csv')
    new_bytes = 0
    source_dir = 'C:\\Users\\jen-chuck\\OneDrive'
    print('Executing backup')
    new_files = 0
    for (dpath, dnames, fnames) in os.walk(source_dir):
        dest_dir = os.path.join(backup_dir, os.path.relpath(dpath, source_dir))
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
                sr = os.stat(file_path)
                if file_path in hash_sources:
                    info = hash_sources[file_path]
                else:
                    info = None
                if info and (sr.st_size == int(info[2])) and (sr.st_mtime_ns == int(info[3])) and \
                        (sr.st_ctime_ns == int(info[4])):
                    hash_val = info[1]
                else:
                    print('Hashing {}'.format(file_path))
                    hash_val = hash_file(file_path)
                    hash_sources[file_path] = [file_path, hash_val, sr.st_size, sr.st_mtime_ns, sr.st_ctime_ns]
                rel_path = os.path.relpath(file_path, source_dir)
                dest_path = os.path.join(backup_dir, rel_path)
                use_copy = True
                if hash_val in hash_targets:
                    # make link
                    # print("Link {} to {}".format(dest_path, hash_targets[hash][0]))
                    try:
                        os.link(hash_targets[hash_val][0], dest_path)
                        use_copy = False
                    except Exception:
                        pass
                if use_copy:
                    # copy new file
                    print('new file {}'.format(file_path))
                    shutil.copy2(file_path, dest_path)
                    sr = os.stat(dest_path)
                    new_bytes += sr.st_size
                    hash_targets[hash_val] = [dest_path, hash_val, sr.st_size, sr.st_mtime_ns, sr.st_ctime_ns]
                    new_files += 1
                    win32api.SetFileAttributes(dest_path, win32con.FILE_ATTRIBUTE_READONLY)
            except Exception:
                pass
    write_file_infos(hash_targets, 'C:\\Backup\\g-hash.csv')
    write_file_infos(hash_sources, 'C:\\Backup\\c-hash.csv')
    for hash_name in ['g-hash.csv', 'c-hash.csv']:
        shutil.copy2(os.path.join('C:\\Backup', hash_name), os.path.join(backup_dir, hash_name))
    return new_files, new_bytes

parser = argparse.ArgumentParser(description='Backup with hardlinks')
check_g_hashes = (len(sys.argv) > 1 and sys.argv[1]=='/check-hashes')
backup_dir = os.path.join('G:\\Backups', date.today().strftime('%Y-%m-%d'))
os.makedirs(backup_dir, exist_ok=True)
new_files = 0
new_bytes = 0
counts = copy_mailbox(backup_dir)
new_files += counts[0]
new_bytes += counts[1]
counts = do_backup(backup_dir, check_g_hashes)
new_files += counts[0]
new_bytes += counts[1]

print('New files: {:,}\nbytes: {:,}'.format(new_files, new_bytes))
