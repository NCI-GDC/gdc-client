from parcel import const, manifest
from ..download.client import GDCHTTPDownloadClient
from ..upload import read_manifest, GDCUploadClient
from .. import defaults
from ..version import __version__
from ..argparser import subparsers
from cmd2 import Cmd, options, make_option
import types
import os
import shlex


class GDCREPL(Cmd):

    HEADER = """
    Type 'help' for a list of commands or 'help <topic>' for detailed usage.
    """.format(version=__version__)

    BASIC_HELP = """Basic help:
    This tool is for downloading data from the GDC.  A few simple usage examples:

    - To simply download an open access file (file_id_1):

        > download file_id_1

    - To simply download a controlled access file (file_id_1) by
      providing the path to a token downloaded from the GDC Portal
      (type 'help token' for more information):

        > download file_id_1 -t path/token.txt

    - To download files from a manifest by providing the path to the
      manifest:

        > download -t path/manifest.txt


    - To upload files from manifest:
    	> upload  path/manifest.txt
        > upload path/resume_manifest.txt

    - To delete files from manifest:
        > delete path/manifest.txt

    - To abort a previous partial upload
        > abort path/resume_manifest.txt
    """

    TIPS = """TIPS:
    - Rather than type out path names, try dragging and dropping manifest and token files into the terminal.
    - You can execute shell commands by prepending '!', i.e. !ls.
    - You can run the gdc-client binary with advanced options from the command line (gdc-client --help).

    """

    BASIC_COMMANDS = """Basic commands are:
    - download   		(download files in registry)
    - add        		(adds ids to registry)
    - list       		(lists file ids already registered)
    - manifest   		(add ids from a GDC manifest file to registry)
    - remove     		(remove ids from registry)
    - token      		(load an authorization token file)
    - cd         		(move to directory you want to download to)
    - pwd        		(print the current working directory)
    - set        		(set advanced configuration setting)
    - settings   		(list advanced configuration settings)
    - upload      		(upload files to object storage)
    - delete            (delete files from object storage)
    - abort             (abort a previous partial upload)
    """

    def __init__(self, *args, **kwargs):
        self.file_ids = set()
        self.token = None
        Cmd.__init__(self, *args, **kwargs)
        print(self.HEADER)
        print(self.BASIC_COMMANDS)
        print(self.TIPS)

        self.settings = dict(
            server=defaults.tcp_url,
            protocol='tcp',
            processes=defaults.processes,
            save_interval=const.SAVE_INTERVAL,
            http_chunk_size=const.HTTP_CHUNK_SIZE,
            download_related_files=True,
            download_annotations=True,
            part_size=defaults.part_size,
            multipart=True,
            verify=True,
        )

    def get(self, setting, stype):
        val = self.settings[setting]
        if isinstance(val, str) and stype == bool:
            return val.lower in ['true', 't']
        else:
            try:
                val = stype(val)
            except:
                raise RuntimeError(
                    "Unable to convert setting '{}'='{}' to type {}".format(
                        setting, val, stype))
        return val

    def format_docstring(self, doc):
        doc = doc.strip() or 'No help available.'
        lines = ['']+[l.strip() for l in doc.split('\n')]
        doc = "\n|    ".join(lines) + '\n'
        return doc

    def _add_ids(self, ids):
        """Adds ids to the instance id list.

        """
        if not ids:
            return
        start_len = len(self.file_ids)
        map(self.file_ids.add, ids)
        end_len = len(self.file_ids)
        print(("Loaded {} new file ids.  There are {} file ids to download.\n"
               "Start download with 'download'.  List ids with 'list'").format(
                   end_len - start_len, end_len))

    def _remove_ids(self, ids):
        """Removes ids from the instance id list.

        """
        if not ids:
            return
        start_len = len(self.file_ids)
        for fid in ids:
            try:
                self.file_ids.remove(fid)
            except Exception as msg:
                print('Unable to remove id {}: {}'.format(fid, msg))
        end_len = len(self.file_ids)
        print(("Removed {} file ids from registry.\n"
               "There are {} file ids left to download.\n"
               "Start download with 'download'.  List ids with 'list'").format(
                   start_len - end_len, end_len))

    def do_manifest(self, manifest_path):
        """Loads a manifest file and adds each id to the instace id list to
        download later

        usage: manifest <path_to_file>

        """
        if not manifest_path:
            print('No manifest specified to load.')
            self.do_help('manifest')
            return
        with open(manifest_path, 'r') as fd:
            self._add_ids([f['id'] for f in manifest.parse(fd)])

    def do_token(self, token_path):
        """Load your token from a file. This token will be used to
        authenticate you when downloading protected data.  You can
        download a token after logging into the GDC Portal at
        https://gdc-portal.nci.nih.gov/

        You can clear the token with the 'clear_token' command.

        useage: token <path_to_file>

        """

        if not token_path:
            print('No token specified to load.')
            if self.token:
                print('Previously loaded token ({} bytes).'.format(
                    len(self.token)))
            else:
                print('No token previously loaded')
            return
        with open(token_path, 'r') as f:
            self.token = f.read().strip()
        print('Loaded token ({} bytes).'.format(len(self.token)))

    def do_list(self, arg):
        """List all ids that have been registered to download.  You can
        download with the 'download' command.  You can add ids to the
        registry with the 'add' command.

        ussage: list

        """
        if not self.file_ids:
            print("No files to download.  Add files with 'manifest' or 'add'.")
        else:
            print('File ids schedule to download:')
            for fid in self.file_ids:
                print(' - {}'.format(fid))

    def do_add(self, arg):
        """Add an id to the registry to be downloaded with the 'download'
        command.  Enter each id separated by a space.

        You can list the ids in the registry with the 'list' command.
        You can remove ids form the registry with the 'remove'
        command.  You can clear all ids from the registry with the
        'clear' command.

        usage: add <id1> [<id2> ...]

        """
        ids = shlex.split(arg)
        if not ids:
            print('No ids specified.')
            self.do_help('add')
            return
        self._add_ids(ids)

    def do_remove(self, arg):
        """Remove ids from the id registry.  Enter each id separated by a
        space.

        You can clear all ids from the registry with the 'clear'
        command.

        usage: add <id1> [<id2> ...]

        """
        ids = shlex.split(arg)
        if not ids:
            print('No ids specified.')
            self.do_help('remove')
            return
        self._remove_ids(ids)

    def do_clear(self, arg):
        """Remove all ids from the id registry.

        You can selectively remove ids from the registry with the 'remove'
        command.

        usage: clear

        """
        self.file_ids = set()
        print('Cleared registered file ids.')

    def do_clear_token(self, arg):
        """Clears the authorization token.

        usage: clear_token

        """
        self.token = None
        print("Cleared authorization token.")

    def do_cd(self, path):
        """Command to change the directory where the files will be downloaded
        to.

        unix usage   : cd temp\downloads
        windosx usage: cd temp/downloads

        """
        os.chdir(os.path.expanduser(path))
        print('Changed working directory to {}'.format(os.getcwd()))

    def do_pwd(self, path):
        """Prints out the current working directory. This is where the data
        will be downloaded.

        You can use the 'cd' command to change directories.

        usage: pwd

        """
        print(os.getcwd())

    @options([
        make_option('-m', '--manifest', help="a manifest file to load ids from"),
        make_option('-t', '--token', help="a token file to load"),
    ])
    def do_download(self, arg, opts=None):
        """Download files that have been registered from (via 'manifest' or
        'add').

        You can specify additional file ids to download in this
        command separated by spaces.

        You can specify a manifest to load from and download using the
        '-m' flag.

        You can specify a token to load from and authenticate with using
        '-t' flag.


        usage: download [<additional_id1> <additional_id2> ... ]
        usage: download -m manifest.txt -t token.txt

        """
        manifest_path = opts.get('manifest')
        token_path = opts.get('token')

        if arg:
            self._add_ids(arg.split())
        if manifest_path:
            self.do_manifest(manifest_path)
        if token_path:
            self.do_token(token_path)
        if not self.file_ids:
            self.do_list(None)
            return

        if self.settings['protocol'] == 'tcp':
            kwargs = dict(
                uri=self.settings['server'],
                token=self.token,
                n_procs=self.get('processes', int),
                directory=os.path.abspath(os.getcwd()),
                http_chunk_size=self.get('http_chunk_size', int),
                save_interval=self.get('save_interval', int),
                download_related_files=self.get('download_related_files', bool),
                download_annotations=self.get('download_annotations', bool),
            )
            client = GDCHTTPDownloadClient(**kwargs)

        elif self.settings['protocol'].lower() == 'udt':
            raise RuntimeError(
                ("UDT protocol not supported in interactive mode.  "
                 "Try running  'gdc-client download -u'"))
        else:
            raise RuntimeError('Protocol ({}) not recognized'.format(
                self.settings['protocol']))

        try:
            downloaded, errors = client.download_files(self.file_ids)
        except Exception as e:
            print('Download aborted: {}'.format(str(e)))
        self._remove_ids(downloaded)

    def _get_upload_client(self, manifest):
        with open(manifest, 'r') as fd:
            files = read_manifest(fd)
            client = GDCUploadClient(
                token=self.token, processes=self.get('processes', int),
                multipart=self.get('multipart', bool),
                part_size=self.get('part_size', int),
                server=self.settings['server'],
                files=files, verify=self.get('verify', bool), manifest_name=manifest)
            return client


    def do_upload(self, manifest):
    	'''upload files given a manifest path'''
        client = self._get_upload_client(manifest)
        client.upload()

    def do_abort(self, manifest):
        '''abort a partially uploaded file'''
        client = self._get_upload_client(manifest)
        client.abort()

    def do_delete(self, manifest):
        '''delete files given a manifest path'''
        client = self._get_upload_client(manifest)
        client.delete()


    def do_help(self, arg):
        """Command to print help message to user.

        """
        if not arg:
            print(self.HEADER)
            print(self.BASIC_COMMANDS)
            print(self.BASIC_HELP)
        else:
            fname = 'do_{}'.format(arg)
            f = getattr(self, fname, None)
            if not f:
                print('help: {} is not a command'.format(fname))
            print self.format_docstring(f.__doc__)

    def do_set(self, arg):
        """Change the value of a setting.

        You can use the 'settings' command to see current settings.

        usage: set <SETTING> <NEW VALUE>

        """
        try:
            attr, value = shlex.split(arg)
        except:
            print('invalid syntax.')
            return self.do_help('set')

        if attr not in self.settings:
            raise ValueError(
                "{} not a valid setting. Try 'settings'.".format(attr))
        print("Updating {} from '{}' to '{}'".format(
            attr, self.settings[attr], value))
        self.settings[attr] = value

    def complete_set(self, text, line, start_index, end_index):
        if text:
            return [key+' ' for key in self.settings if key.startswith(text)]
        else:
            return self.settings.keys()

    def do_settings(self, arg):
        """Lists the current settings and their values.

        You can update the value of these settings with the 'set' command.

        usage: settings

        """
        print('-- Settings --')
        pad = max([len(key) for key in self.settings])+1
        for key, val in self.settings.iteritems():
            print('{}: {}'.format(key.ljust(pad), val))

    def do_show(self, arg):
        """Alias for 'settings' commands.

        """
        self.do_settings(arg)

    def do_commands(self, arg):
        """Alias for 'help' command.

        """
        self.do_help(arg)

def run_repl(args):
    r = GDCREPL()
    r.prompt = '\ngdc-client repl > '
    r.cmdloop()




def main():
    args = subparsers.parse_args()
    if args.verbose:
        logging.root.setLevel(logging.DEBUG)

    # If there are arguments other than subcommand, run cli
    if sys.argv[2:]:
        try:
            run_cli(args)
        except Exception as e:
            if args.debug:
                raise
            else:
                print('Process aborted: {}'.format(str(e)))

    # Else, run as a repl
    else:
        run_repl(args)
