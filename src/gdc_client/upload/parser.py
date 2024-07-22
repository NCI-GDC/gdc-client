import argparse
import logging
from functools import partial

from gdc_client.upload import manifest
from gdc_client.upload import exceptions
from gdc_client.upload.client import GDCUploadClient


log = logging.getLogger("gdc-upload")


def validate_args(parser, args):
    """Validate argparse namespace."""
    if args.part_size:
        log.warning("[--part-size] is DEPRECATED in favor of [--upload-part-size]")
        args.upload_part_size = args.part_size

    if not args.token_file:
        parser.error("A token is required in order to upload.")

    if args.manifest or args.file_ids:
        return

    parser.error("must specify either --manifest or file_id(s)")


def upload(parser, args):
    """Upload data to the GDC."""

    validate_args(parser, args)

    files = manifest.load(args.manifest)["files"] if args.manifest else []

    for f in files:
        # empty string if nothing else
        f["path"] = args.path

    if not args.manifest:
        for uuid in args.file_ids:
            files.append(
                {
                    "id": uuid,
                    "project_id": args.project_id,
                    "path": args.path,
                    "upload_id": args.upload_id,
                }
            )

    manifest_name = args.manifest.name if args.manifest else args.file_ids[0]

    client = GDCUploadClient(
        token=args.token_file,
        processes=args.n_processes,
        multipart=(not args.disable_multipart),
        upload_part_size=args.upload_part_size,
        server=args.server,
        files=files,
        verify=(not args.insecure),
        manifest_name=manifest_name,
    )

    if args.abort:
        client.abort()
    elif args.delete:
        client.delete()
    else:
        client.upload()


def config(parser, upload_defaults):
    """Configure a parser for upload."""
    func = partial(upload, parser)
    upload_defaults["func"] = func

    parser.set_defaults(**upload_defaults)

    parser.add_argument(
        "--project-id", "-p", type=str, help="The project ID that owns the file"
    )
    parser.add_argument(
        "--path", "-f", metavar="path", type=str, help="directory path to find file"
    )
    parser.add_argument("--upload-id", "-u", help="Multipart upload id")
    parser.add_argument(
        "--insecure",
        "-k",
        action="store_true",
        help="Allow connections to server without certs",
    )
    # TODO remove this and replace w/ top level host and port
    parser.add_argument("--server", "-s", type=str, help="GDC API server address")
    parser.add_argument(
        "--part-size", type=int, help="DEPRECATED in favor of [--upload-part-size]"
    )
    parser.add_argument(
        "--upload-part-size", "-c", type=int, help="Part size for multipart upload"
    )
    parser.add_argument(
        "-n", "--n-processes", type=int, help="Number of client connections"
    )
    parser.add_argument(
        "--disable-multipart", action="store_true", help="Disable multipart upload"
    )
    parser.add_argument(
        "--abort", action="store_true", help="Abort previous multipart upload"
    )
    parser.add_argument(
        "--resume", "-r", action="store_true", help="Resume previous multipart upload"
    )
    parser.add_argument("--delete", action="store_true", help="Delete an uploaded file")
    parser.add_argument(
        "--manifest",
        "-m",
        type=argparse.FileType("r"),
        help="Manifest which describes files to be uploaded",
    )
    parser.add_argument(
        "file_ids",
        metavar="file_id",
        type=str,
        nargs="*",
        help="The GDC UUID of the file(s) to upload",
    )
    parser.add_argument("--config", help="Path to INI-type config file", metavar="FILE")
