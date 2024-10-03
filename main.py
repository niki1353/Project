import pandas as pd
from elasticsearch import Elasticsearch, NotFoundError

class EmployeeDatabase:
    def __init__(self, es_host):
        self.es = Elasticsearch([es_host])

    def create_collection(self, collection_name):
        if not self.es.indices.exists(index=collection_name):
            mappings = {
                "properties": {
                    "Employee ID": {"type": "keyword"},
                    "Full Name": {"type": "text"},
                    "Job Title": {"type": "text"},
                    "Department": {"type": "keyword"},
                    "Business Unit": {"type": "text"},
                    "Gender": {"type": "keyword"},
                    "Ethnicity": {"type": "keyword"},
                    "Age": {"type": "integer"},
                    "Hire Date": {"type": "date"},
                    "Annual Salary": {"type": "float"},
                    "Bonus %": {"type": "float"},
                    "Country": {"type": "text"},
                    "City": {"type": "text"},
                    "Exit Date": {"type": "date"},
                }
            }
            self.es.indices.create(index=collection_name, mappings=mappings)
            print(f"Collection '{collection_name}' created.")

    def clean_data(self, df):
        """Clean the DataFrame for Elasticsearch indexing."""
        df['Annual Salary'] = df['Annual Salary'].replace({r'\$': '', r',': ''}, regex=True).astype(float)
        df['Bonus %'] = df['Bonus %'].replace({'%': ''}, regex=True).astype(float)
        df['Hire Date'] = pd.to_datetime(df['Hire Date'], errors='coerce')
        df['Exit Date'] = pd.to_datetime(df['Exit Date'], errors='coerce')
        df['Full Name'] = df['Full Name'].str.replace(r'[^\w\s]', '', regex=True)
        df['Job Title'] = df['Job Title'].str.replace(r'[^\w\s]', '', regex=True)
        return df

    def index_data(self, collection_name, exclude_column):
        df = pd.read_csv('employee_data.csv', encoding='ISO-8859-1')
        df = df.drop(columns=[exclude_column])
        df = df.drop_duplicates(subset=['Employee ID'])
        df = self.clean_data(df)
        
        if df.isnull().values.any():
            print("DataFrame contains null values. Please check your CSV.")
            return

        for _, document in df.iterrows():
            employee_id = document['Employee ID']
            if pd.isnull(employee_id):
                print("Skipping document with null Employee ID")
                continue
            self.es.index(index=collection_name, id=employee_id, document=document.to_dict())
            print(f"Indexed document with Employee ID: {employee_id}")

    def search_by_column(self, collection_name, column_name, column_value):
        query = {
            "query": {
                "match": {
                    column_name: column_value
                }
            }
        }
        response = self.es.search(index=collection_name, body=query)
        return response['hits']['hits']

    def get_emp_count(self, collection_name):
        return self.es.count(index=collection_name)['count']

    def del_emp_by_id(self, collection_name, employee_id):
        try:
            self.es.delete(index=collection_name, id=employee_id)
            print(f"Deleted employee with ID: {employee_id}")
        except NotFoundError:
            print(f"Employee ID: {employee_id} not found.")

    def get_dep_facet(self, collection_name):
        query = {
            "size": 0,
            "aggs": {
                "department_count": {
                    "terms": {
                        "field": "Department",
                        "size": 10
                    }
                }
            }
        }
        response = self.es.search(index=collection_name, body=query)
        return response['aggregations']['department_count']['buckets']


if __name__ == "__main__":
    es_host = 'http://localhost:9200'
    db = EmployeeDatabase(es_host)

    v_name_collection = 'hash_nikhil'
    v_phone_collection = 'hash_6789'
    
    db.create_collection(v_name_collection)
    db.create_collection(v_phone_collection)
    
    emp_count_name = db.get_emp_count(v_name_collection)
    print(f"Employee count in '{v_name_collection}': {emp_count_name}")
    
    db.index_data(v_name_collection, 'Department')
    db.index_data(v_phone_collection, 'Gender')

    db.del_emp_by_id(v_name_collection, 'E02003')

    emp_count_after_delete = db.get_emp_count(v_name_collection)
    print(f"Employee count in '{v_name_collection}' after deletion: {emp_count_after_delete}")

    it_employees = db.search_by_column(v_name_collection, 'Department', 'IT')
    print(f"Employees in IT department: {it_employees}")

    male_employees = db.search_by_column(v_name_collection, 'Gender', 'Male')
    print(f"Male employees: {male_employees}")

    it_employees_phone = db.search_by_column(v_phone_collection, 'Department', 'IT')
    print(f"Employees in IT department for phone collection: {it_employees_phone}")

    dep_facet_name = db.get_dep_facet(v_name_collection)
    print(f"Department facets for '{v_name_collection}': {dep_facet_name}")

    dep_facet_phone = db.get_dep_facet(v_phone_collection)
    print(f"Department facets for '{v_phone_collection}': {dep_facet_phone}")
