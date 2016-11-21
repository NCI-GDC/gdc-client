
from unittest import TestCase

from gdc_client.upload.client import create_resume_path

class UploadClientTest(TestCase):
    def setup(self):
        pass

    def test_create_resume_path(self):
        # don't need to test if there's no file given
        # that is checked in multipart_upload()
        tests = [
                '/path/to/file.yml',
                'path/to/file.yml',
                'file.yml']
        results =[
                '/path/to/resume_file.yml',
                'path/to/resume_file.yml',
                'resume_file.yml']
        for i, t in enumerate(tests):
            assert create_resume_path(t) == results[i]

