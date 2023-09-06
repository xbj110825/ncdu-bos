import click
import time
import json
import itertools
from typing import IO
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient


class NcduDataWriter(object):
    """
    A class to write ncdu formatted data files (like ncdu -o).
    """

    def __init__(self, output: IO, root: str, progname: str, progver: str):
        """
        Initialize the NcduDataWriter instance.

        Parameters:
            output: IO object to write the data.
            root: The root directory name.
        """
        
        self.output = output
        self.depth = 0

        self.output.write('[1,0,')
        json.dump({'progname': progname, 'progver': progver,
                  'timestamp': int(time.time())}, self.output)

        # ncdu data format must begin with a directory
        self.dir_enter(root)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def dir_enter(self, name: str):
        """
        Enter a directory.

        Parameters:
            name: The directory name.
        """
        self.depth += 1
        self.output.write(",\n")
        self.output.write('[')
        json.dump({'name': name}, self.output)

    def dir_leave(self):
        """Leave a directory."""
        if self.depth > 0:
            self.depth -= 1
            self.output.write(']')

    def file_entry(self, name: str, size: int):
        self.output.write(",\n")
        json.dump({'name': name, 'dsize': size}, self.output)

    def close(self):
        for i in range(self.depth):
            self.dir_leave()

        # close the format JSON document we opened in our constructor
        self.output.write(']')


class DirectoryWalker(object):
    def __init__(self, writer: NcduDataWriter):
        self.writer = writer
        self.current_path_parts = []

    def process_item(self, path: str, size: int):
        path_parts = path.split('/')
        key_filename = path_parts.pop()

        if self.current_path_parts != path_parts:
            # update our position in the directory hierarchy
            conflict = False
            add_dirs = []

            for p1, p2 in itertools.zip_longest(self.current_path_parts, path_parts):
                if p1 != p2:
                    # first conflict starts another logic in our code
                    conflict = True

                if conflict:
                    if p1 is not None:
                        self.writer.dir_leave()

                    if p2 is not None:
                        add_dirs.append(p2)

            for d in add_dirs:
                # ncdu doesn't support empty dir names. Replace '' with '<empty>'
                self.writer.dir_enter(d if d != '' else '<empty>')

            self.current_path_parts = path_parts

        # directory entry ends with a '/' so the key_filename will be ''.
        # in that case, omit it
        if key_filename != '':
            self.writer.file_entry(key_filename, size)


class BosDirectoryGenerator(object):
    def __init__(self, endpoint: str, access_key_id: str, secret_access_key: str, bucket: str, prefix: str, max_keys: int = 1000):
        config = BceClientConfiguration(credentials=BceCredentials(
            access_key_id, secret_access_key), endpoint=endpoint)
        self.bos_client = BosClient(config)
        self.bucket = bucket
        self.prefix = prefix
        self.max_keys = max_keys

    def __iter__(self):
        return self.generator()

    def generator(self):
        is_truncated = True
        marker = None
        while is_truncated:
            response = self.bos_client.list_objects(
                self.bucket, prefix=self.prefix, marker=marker, max_keys=self.max_keys)
            for obj in response.contents:
                yield (obj.key, obj.size)
            is_truncated = response.is_truncated
            marker = getattr(response, 'next_marker', None)


@click.command()
@click.option('--endpoint', required=True)
@click.option('--access-key-id', prompt=True, envvar="BOS_ACCESS_KEY_ID", hide_input=True)
@click.option('--secret-access-key', prompt=True, envvar="BOS_SECRET_ACCESS_KEY", hide_input=True)
@click.option('--bucket', required=True)
@click.option('--prefix', default='')
@click.option('--output', required=True, type=click.File('w'), default='ncdu.json')
def main(endpoint: str, access_key_id: str, secret_access_key: str, bucket: str, prefix: str, output: IO):
    bos_directory_generator = BosDirectoryGenerator(
        endpoint=endpoint, access_key_id=access_key_id, secret_access_key=secret_access_key, bucket=bucket, prefix=prefix)

    with NcduDataWriter(output, bucket, progname='ncdu-bos', progver='0.1') as ncdu:
        walker = DirectoryWalker(ncdu)

        for path, size in bos_directory_generator:
            walker.process_item(path, size)


if __name__ == '__main__':
    main()
