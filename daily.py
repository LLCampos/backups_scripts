import json
import os

import yaml

import external.backup_goodreads
from external.DownloadTable import DownloadTable


with open("conf.yaml", 'r') as stream:
    config = yaml.safe_load(stream)


def backup_goodreads():
    print("Backing up Goodreads...")
    user_id = config["goodreads"]["user_id"]
    api_key = config["goodreads"]["api_key"]
    backups_dir = config["backups_dir"]
    reviews = list(external.backup_goodreads.get_reviews(user_id, api_key))
    external.backup_goodreads.write_reviews_to_disk(reviews, backups_dir)
    print("Done!")


def backup_airtable():
    print("Backing up Airtable...")
    backups_dir = config["backups_dir"]
    api_key = config["airtable"]["api_key"]
    bases = config["airtable"]["bases"]

    for base in bases:
        base_name = base["name"]
        base_key = base["key"]
        tables_names = base["tables"]

        print(f"Backing up base '{base_name}'...")

        for table_name in tables_names:
            download_table = DownloadTable(base_key, table_name, api_key, compression=False, discard_attach=True)
            table = list(download_table.download())
            json_str = json.dumps(table, indent=2, sort_keys=True)
            file_path = os.path.join(f"{backups_dir}/airtable/{base_name}_{table_name}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_str)

        print("Done!")


if __name__ == '__main__':
    backup_goodreads()
    backup_airtable()
