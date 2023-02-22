import os
import sys
import re
import json
import requests
from glob import glob
from time import time
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import signal

DOWNLOAD_FILE_NON_200, DOWNLOAD_FILE_WRITE_ERR, DOWNLOAD_FILE_FILENAME_ERR = -1, -2, -3

def download_file(url, base_dir, include_time = True):
    print(f'Downloading {url}... ', end='')

    response = requests.get(url)
    if (response.status_code != 200):
        print('non 200 response')
        return DOWNLOAD_FILE_NON_200

    try:
        filename = get_filename(url, include_time)
    except:
        print('could not get filename')
        return DOWNLOAD_FILE_FILENAME_ERR

    filepath = os.path.join(base_dir, filename)
    try:
        with open(filepath, 'wb') as file:
            file.write(response.content)
    except:
        print('could not write file')
        return DOWNLOAD_FILE_WRITE_ERR

    print('done')
    return filename


def get_filename(url, include_time):
    url_comp = urlparse(url)
    [(f_path, f_ext)] = re.findall(r"(.*)\.(\w+)$", url_comp.path)
    size_tuple = re.findall(r"size=(\d+x\d+)", url_comp.query)
    comp = (
        f_path.replace("/", '_').lstrip("_"),
        size_tuple[0] if size_tuple else None,
        str(int(time())) if include_time else None,
        f_ext)
    return '.'.join([c for c in comp if c])


class Processor:
    STATE_VER = 1

    def __init__(self, script_dir, data_dir):
        self.state_file = os.path.join(script_dir, 'state.json')
        self.data_dir = data_dir
        self.should_stop = False

        if os.path.isfile(self.state_file):
            self.state = self.read_state()
            if self.state["version"] != self.STATE_VER:
                print("state version mistmatch")
                sys.exit(1)
        else:
            self.state = {
                "version": self.STATE_VER,
                "remaining_files": [],
                "processed_files": [],
            }
            print("Starting from scratch...")
            self.find_all_messages()

    def resume(self):
        while True:
            next_file = self.get_next_file()

            if not next_file:
                if self.should_stop:
                    print("Stopped by Ctrl-C")
                else:
                    print("All done")
                sys.exit(0)

            next_filepath = os.path.join(self.data_dir, next_file)
            html_dir, html_filename = os.path.split(next_filepath)

            print(f"Processing {next_file}...")
            with open(next_filepath, 'r', encoding="cp1251") as f:
                data = f.read()
            soup = BeautifulSoup(data, features="html.parser")

            links = soup.findAll('a', attrs={"class": "attachment__link"})
            replaced_links = 0

            if len(links):
                for link in links:
                    if re.findall(r"\.(apng|avif|svg|webp|jpe?g|png|mng|gif|bmp|ico|tiff)(\?|$)", link["href"]):
                        replaced_links += 1
                        img_filename = download_file(link["href"], html_dir)

                        if img_filename == DOWNLOAD_FILE_NON_200:
                            p_tag = soup.new_tag('p')
                            p_tag.string = f"Couldn't download on{datetime.now().strftime('%Y-%m-%d %T')}"
                            link.insert_after(p_tag)
                            continue
                        elif type(img_filename) == int:
                            sys.exit(1)

                        img_tag = soup.new_tag('img', src=img_filename)
                        link.insert_after(img_tag)

                if replaced_links > 0:
                    rename_to = re.sub(r'\.html$', '.html~', html_filename)
                    try:
                        os.rename(next_filepath, os.path.join(html_dir, rename_to))
                    except:
                        print("couldn't rename file")

                    with open(next_filepath, 'w', encoding="utf8") as f:
                        f.write(str(soup))

                print(f"Replaced {replaced_links} links\n")
            else:
                print("No links found")

            # Loop
            self.mark_as_done(next_file)


    def get_next_file(self):
        if self.should_stop:
            return None

        remaining_len = len(self.state["remaining_files"])
        if remaining_len:
            print(f"{remaining_len} files left...")
            return self.state["remaining_files"][0]
        return None

    def find_all_messages(self):
        all_messages = glob(os.path.join(self.data_dir, 'messages/**/*.html'))
        self.state["remaining_files"] = [m.replace(self.data_dir, '').lstrip('/') for m in all_messages]
        print(f"Found {len(all_messages)} files...")
        self.write_state()

    def mark_as_done(self, filename):
        print(f'Remove {filename}')
        self.state["processed_files"].append(filename)
        self.state["remaining_files"].remove(filename)
        self.write_state()

    def write_state(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)
        print("State persisted!")

    def read_state(self):
        print("State loaded from file...")
        with open(self.state_file, 'r') as f:
            return json.load(f)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("process.py <vk_archive_root_dir>")
        sys.exit(1)

    processor = Processor(
        script_dir=os.path.dirname(os.path.realpath(__file__)),
        data_dir=sys.argv[1]
    )

    def sigint_handler(signum, frame):
        print("\n\n")
        res = input("Stop? y/n ")
        if res == 'y':
            processor.should_stop = True

    signal.signal(signal.SIGINT, sigint_handler)

    processor.resume()
