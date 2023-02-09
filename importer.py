import models
from models import Semmed
import csv
import os


def insert_records(filename):
    conn = models.session()
    total_counter = 0
    with open(filename, 'r') as infile:
        with open('rejects.csv', 'w') as errorfile:
            csv_reader = csv.reader(infile, dialect='excel')
            buffer = []
            for row in csv_reader:
                if row[0] == "PMID":
                    continue
                if not row[1].isdigit() or \
                        not row[3].isdigit() or \
                        not row[9].isdigit() or \
                        not row[10].isdigit() or \
                        not row[11].isdigit() or \
                        not row[12].isdigit() or \
                        len(row[5]) > 40 or \
                        len(row[7]) > 40 or \
                        '|' in row[5] or \
                        '|' in row[7]:
                    x = errorfile.write(','.join(row) + '\n')
                    continue
                buffer.append({
                    'pmid': 'PMID:' + row[0],
                    'sen_start_index': row[1],
                    'sentence': row[2],
                    'sen_end_index': row[3],
                    'predicate': row[4],
                    'subject_cui': 'UMLS:' + row[5],
                    'subject_name': row[6],
                    'object_cui': 'UMLS:' + row[7],
                    'object_name': row[8],
                    'subject_start_index': row[9],
                    'subject_end_index': row[10],
                    'predicate_start_index': row[11],
                    'predicate_end_index': row[12]
                })
                if len(buffer) > 100000:
                    print(f"Inserting {len(buffer)} records")
                    conn.bulk_insert_mappings(Semmed, buffer)
                    total_counter = total_counter + len(buffer)
                    buffer = []
                    print(f"Total records so far: {total_counter}")
            conn.bulk_insert_mappings(Semmed, buffer)
    conn.commit()


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\\prod-creds.json'
    models.init_db(instance='translator-text-workflow-dev:us-central1:text-mined-assertions-prod',
                   database='text_mined_assertions')
    insert_records("merged_tables.csv")
