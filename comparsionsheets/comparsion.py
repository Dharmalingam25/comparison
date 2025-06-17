import pandas as pd
import mysql.connector
from datetime import datetime


class TimesheetComparator:
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'root',
            'database': 'associatedata'
        }
        self.conn = None

    def connect_db(self):
        self.conn = mysql.connector.connect(**self.db_config)

    def close_db(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()

    def get_associates(self):
        try:
            self.connect_db()
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM associates")
            return cursor.fetchall()
        except Exception as e:
            print(f"Error fetching associates: {e}")
            return []
        finally:
            self.close_db()

    def compare_timesheets(self, company_file, client_file):
        try:
            # Load associates from DB
            associates = self.get_associates()
            if not associates:
                print("No associate data found.")
                return

            associates_df = pd.DataFrame(associates)
            associates_df['associate_id'] = associates_df['CTSID'].astype(str).str.strip()
            associates_df['external_id'] = associates_df['ExternalID'].astype(str).str.strip()
            associates_df['project_name'] = associates_df['ProjectDescription']

            id_to_associate = {
                row['associate_id']: row for _, row in associates_df.iterrows()
            }
            ext_id_to_associate = {
                row['external_id']: row for _, row in associates_df.iterrows()
            }

            # Load timesheets
            company_df = pd.read_excel(company_file)
            client_df = pd.read_excel(client_file)

            # Clean columns
            company_df.columns = [col.strip().lower() for col in company_df.columns]
            client_df.columns = [col.strip().lower() for col in client_df.columns]

            # Rename for uniformity
            company_df = company_df.rename(columns={
                'associate id': 'associate_id',
                'reporting date': 'date',
                'project name': 'project_name',
                'time quantity': 'hours'
            })
            client_df = client_df.rename(columns={
                'external id': 'external_id',
                'date': 'date',
                'units': 'hours'
            })

            # Clean and convert
            company_df['associate_id'] = company_df['associate_id'].astype(str).str.strip()
            client_df['external_id'] = client_df['external_id'].astype(str).str.strip()
            company_df['date'] = pd.to_datetime(company_df['date'], errors='coerce')
            client_df['date'] = pd.to_datetime(client_df['date'], errors='coerce')

            # Add project and associate ID to client data
            client_df['project_name'] = client_df['external_id'].map(
                lambda eid: ext_id_to_associate.get(eid, {}).get('project_name', 'Unknown')
            )
            client_df['associate_id'] = client_df['external_id'].map(
                lambda eid: ext_id_to_associate.get(eid, {}).get('associate_id', None)
            )

            # Project-level summary
            print("\n=== PROJECT LEVEL COMPARISON ===")
            projects = company_df['project_name'].dropna().unique()
            for project in projects:
                comp_proj = company_df[company_df['project_name'] == project]
                cli_proj = client_df[client_df['project_name'] == project]
                assoc_ids = comp_proj['associate_id'].unique()

                print(f"\nProject: {project}")
                print(f"Total Company Hours: {comp_proj['hours'].sum()}")
                print(f"Total Client Hours: {cli_proj['hours'].sum()}")
                print(f"Total Associates: {len(assoc_ids)}")

                mismatch_count = 0
                for aid in assoc_ids:
                    ch = comp_proj[comp_proj['associate_id'] == aid]['hours'].sum()
                    clh = cli_proj[cli_proj['associate_id'] == aid]['hours'].sum()
                    status = 'Matched' if ch == clh else 'Mismatch'
                    if status == 'Mismatch':
                        mismatch_count += 1
                    name = id_to_associate.get(aid, {}).get('ContractorName', 'Unknown')
                    # print(f"  - {name} ({aid}): Company={ch}, Client={clh}, Status={status}")
                print(f"Mismatch Count: {mismatch_count}")

                # ASSOCIATE LEVEL COMPARISON WITH PROJECT GROUPING
                print("\n=== ASSOCIATE LEVEL COMPARISON ===")
                all_projects = sorted(company_df['project_name'].dropna().unique())

                for project in all_projects:
                    print(f"\nProject: {project}")
                    print("-" * 84)
                    print(
                        f"{'Associate Name':<27} | {'ID':<10} | {'Company Hours':>13} | {'Client Hours':>13} | {'Status':<10}")
                    print("-" * 84)

                    # Get associates in project
                    comp_proj = company_df[company_df['project_name'] == project]
                    cli_proj = client_df[client_df['project_name'] == project]

                    associate_ids = comp_proj['associate_id'].unique()

                    for aid in associate_ids:
                        name = id_to_associate.get(aid, {}).get('ContractorName', 'Unknown')
                        ch = comp_proj[comp_proj['associate_id'] == aid]['hours'].sum()
                        clh = cli_proj[cli_proj['associate_id'] == aid]['hours'].sum()
                        status = 'Matched' if ch == clh else 'Mismatch'

                        print(f"{name:<27} | {aid:<10} | {ch:>13.2f} | {clh:>13.2f} | {status:<10}")
            # # Date-level breakdown
            # print("\n=== DATE LEVEL COMPARISON ===")
            # for project in projects:
            #     comp_proj = company_df[company_df['project_name'] == project]
            #     cli_proj = client_df[client_df['project_name'] == project]
            #     assoc_ids = comp_proj['associate_id'].unique()
            #
            #     for aid in assoc_ids:
            #         name = id_to_associate.get(aid, {}).get('ContractorName', 'Unknown')
            #         print(f"\nProject: {project} - {name} ({aid})")
            #         print(f"{'Date':<12} | {'Company Hours':>13} | {'Client Hours':>13}")
            #         print("-" * 45)
            #
            #         comp_dates = comp_proj[comp_proj['associate_id'] == aid]
            #         cli_dates = cli_proj[cli_proj['associate_id'] == aid]
            #         all_dates = sorted(set(comp_dates['date']).union(set(cli_dates['date'])))
            #
            #         for d in all_dates:
            #             ch = comp_dates[comp_dates['date'] == d]['hours'].sum()
            #             clh = cli_dates[cli_dates['date'] == d]['hours'].sum()
            #             print(f"{d.strftime('%Y-%m-%d'):<12} | {ch:>13.2f} | {clh:>13.2f}")

        except Exception as e:
            print(f"Error comparing timesheets: {e}")


def main():
    comparator = TimesheetComparator()
    #comparator.compare_timesheets('source/Cognizant TS.xlsx', 'source/Client timesheet.xlsx')
    comparator.compare_timesheets('source/src/testcompanyts.xlsx', 'source/src/Client TS - May25.xlsx')



if __name__ == '__main__':
    main()