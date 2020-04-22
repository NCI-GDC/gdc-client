UPLOAD_MANIFEST_SCHEMA = {
    "title": "Manifest Schema",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "files": {
            "items": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
                    },
                    "file_name": {"type": "string"},
                    "local_file_path": {"type": "string"},
                    "file_size": {"type": "integer"},
                    "md5sum": {"type": "string", "pattern": "^[a-f0-9]{32}$"},
                    "type": {"type": "string"},
                    "project_id": {"type": "string"},
                },
                "anyOf": [
                    {"required": ["id", "file_name", "project_id"]},
                    {"required": ["id", "local_file_path", "project_id"]},
                ],
            }
        }
    },
}
