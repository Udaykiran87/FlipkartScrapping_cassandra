from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json
from cassandra.query import BatchStatement

# #Connect to Astra Cluster
# #Redo this after git project restructuring
# with open('astra.credentials/UserCred.json') as f:
#     cred = json.load(f)
# cloud_config= {
#         'secure_connect_bundle': 'astra.credentials/secure-connect-'+cred['cluster'].replace("_","-")+'.zip'
# }
# auth_provider = PlainTextAuthProvider(cred['username'], cred['password'])
# cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
# session = cluster.connect()
# row = session.execute("select release_version from system.local").one()
# print(row[0])
# rows = session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '"+cred['keyspace']+"'")
#
# result = []
# for row in rows:
#     print(row[0])
#     print(row[1])
#     print(cred['table'])
#     if row[1] == cred['table']:
#         print(True)
# print(False)
# # print(result)
# print(rows[0])
# print(rows[1])
#
# rows = session.execute("Drop Table " + cred['keyspace'] + "." + cred['table'])
# for row in rows:
#     print(row[0])
#     print(row[1])
#     print(cred['table'])
#     if row[1] == cred['table']:
#         print(True)
#
# #Create Table leaves if it does not exist
# session.set_keyspace(cred['keyspace'])
# f = open('astra.import/Leaves.Astra.cql')
# exec_command = str(f.read())
# exec_command = exec_command.replace('keyspace_name',cred['keyspace'],1)
# exec_command = exec_command.replace('table_name',cred['table'],1)
# print(exec_command)
# session.execute(exec_command).one()
#
#
#
# rows = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table'])
# #print(type(rows))
# print(rows)
# result = []
# for row in rows:
# 	#print(type(str(row)))
# 	result.append(json.loads(row.json))
# print(result)
#
# table_exist = "SELECT *  FROM system_schema.tables WHERE keyspace_name = cred['keyspace'];"
# session.execute(table_exist).one()

class CassandraManagement:

    def __init__(self):
        """
        This function sets the required url
        """
        try:
            with open('astra.credentials/UserCred.json') as f:
                self.cred = json.load(f)
            self.cloud_config = {
                'secure_connect_bundle': 'astra.credentials/secure-connect-' + self.cred['cluster'].replace("_",
                                                                                                       "-") + '.zip'
            }
            self.auth_provider = PlainTextAuthProvider(self.cred['username'], self.cred['password'])
        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))

    def getCassandraSessionObject(self):
        """
        This functicloseCassandraSessionObjecton creates the client object for connection purpose
        """
        try:
            self.cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
            self.session = self.cluster.connect()
            return self.session
        except Exception as e:
            raise Exception("(getCassandraSessionObject): Something went wrong on creation of client object\n" + str(e))

    def closeCassandraSessionObject(self):
        """
        This function closes the connection of client
        :return:
        """
        try:
            self.cluster.shutdown()
            self.session.shutdown()
        except Exception as e:
            raise Exception(f"Something went wrong on closing connection\n" + str(e))

    def isKeyspacePresent(self, keyspace_name):
        """
        This function checks if the database is present or not.
        :param keyspace_name:
        :return:
        """
        try:
            session_ = self.getCassandraSessionObject()
            rw = session_.execute("Select keyspace_name from system_schema.keyspaces;")
            for i in rw:
                if keyspace_name in i:
                    return True
            return False
        except Exception as e:
            raise Exception("(isKeyspacePresent): Failed on checking if the keyspace is present or not \n" + str(e))

    # def createKeyspace(self, keyspace_name):
    #     """
    #     This function creates database.
    #     :param db_name:
    #     :return:
    #     """
    #     try:
    #         keyspace_check_status = self.isKeyspacePresent(keyspace_name=keyspace_name)
    #         if not keyspace_check_status:
    #             session_ = self.getCassandraSessionObject()
    #             query = "CREATE KEYSPACE keyspace_name WITH replication = {'class':'SimpleStrategy','replication_factor':1};"
    #             keyspace = session_.execute(query)
    #             return keyspace
    #     except Exception as e:
    #         raise Exception(f"(createKeyspace): Failed on creating keyspace\n" + str(e))
    #
    # def dropKeyspace(self, keyspace_name):
    #     """
    #     This function deletes the database from MongoDB
    #     :param db_name:
    #     :return:
    #     """
    #     try:
    #         keyspace_check_status = self.isKeyspacePresent(keyspace_name=keyspace_name)
    #         if not keyspace_check_status:
    #             session_ = self.getCassandraSessionObject()
    #             query = "DROP  keyspace" + keyspace_name
    #             keyspace = session_.execute(query)
    #             return f"keyspace is dropped"
    #     except Exception as e:
    #         raise Exception(f"(keyspace): Failed to delete database {keyspace_name}\n" + str(e))

    # def getDatabase(self, db_name):
    #     """
    #     This returns databases.
    #     """
    #     try:
    #         mongo_client = self.getCassandraSessionObject()
    #         mongo_client.close()
    #         return mongo_client[db_name]
    #     except Exception as e:
    #         raise Exception(f"(getDatabase): Failed to get the database list")
    #
    # def getCollection(self, collection_name, db_name):
    #     """
    #     This returns collection.
    #     :return:
    #     """
    #     try:
    #         database = self.getDatabase(db_name)
    #         return database[collection_name]
    #     except Exception as e:
    #         raise Exception(f"(getCollection): Failed to get the database list.")
    #
    def isTablePresent(self, keyspace_name, table_name):
        """
        This checks if collection is present or not.
        :param keyspace_name:
        :param table_name:
        :return:
        """
        try:
            # keyspace_name = cred['keyspace']
            keyspace_status = self.isKeyspacePresent(keyspace_name=keyspace_name)
            if keyspace_status:
                rows = self.session.execute(
                    "SELECT * FROM system_schema.tables WHERE keyspace_name = '" + keyspace_name + "'")
                for row in rows:
                    if row[1] == table_name:
                        return True
                return False
            else:
                return False
        except Exception as e:
            raise Exception(f"(isTablePresent): Failed to check table\n" + str(e))
    #
    def createTable(self, keyspace_name, table_name):
        """
        This function creates the collection in the database given.
        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            keyspace_check_status = self.isKeyspacePresent(keyspace_name=keyspace_name)
            if keyspace_check_status:
                session_ = self.getCassandraSessionObject()
                # Create Table cred['table'] if it does not exist
                session_.set_keyspace(keyspace_name)
                f = open('astra.import/Leaves.Astra.cql')
                exec_command = str(f.read())
                exec_command = exec_command.replace('keyspace_name', keyspace_name, 1)
                exec_command = exec_command.replace('table_name', table_name, 1)
                self.session.execute(exec_command).one()

        except Exception as e:
            raise Exception(f"(createCollection): Failed to create collection {table_name}\n" + str(e))

    def dropTable(self, keyspace_name, table_name):
        """
        This function drops the collection
        :param collection_name:
        :param db_name:
        :return:
        """
        try:
            table_check_status = self.isTablePresent(self, keyspace_name, table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                rows = session_.execute("Drop Table " + keyspace_name + "." + table_name)
                return True
            else:
                return False
        except Exception as e:
            raise Exception(f"(dropCollection): Failed to drop table {table_name}")

    def insertRecord(self, keyspace_name, table_name, record):
        """
        This inserts a record.
        :param db_name:
        :param collection_name:
        :param record:
        :return:
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if not table_check_status:
                self.createTable(keyspace_name=keyspace_name, table_name=table_name)
            session_ = self.getCassandraSessionObject()
            # Create Table cred['table'] if it does not exist
            session_.set_keyspace(keyspace_name)
            insert_record_query = "insert into " + keyspace_name + "." + table_name + """ (id, product_name, product_searched, price, offer_details, discount_percent, emi, rating,comment, customer_name, review_age) values (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"""
            records_to_insert = tuple(record.values())
            session_.execute(insert_record_query, records_to_insert).one()
            return f"rows inserted "
        except Exception as e:
            raise Exception(f"(insertRecord): Something went wrong on inserting record\n" + str(e))

    def insertRecords(self, keyspace_name, table_name, records):
        """
        This inserts a record.
        :param db_name:
        :param collection_name:
        :param record:
        :return:
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if not table_check_status:
                self.createTable(keyspace_name=keyspace_name, table_name=table_name)
            session_ = self.getCassandraSessionObject()
            # Create Table cred['table'] if it does not exist
            session_.set_keyspace(keyspace_name)
            insert_record_query = "insert into " + keyspace_name + "." + table_name + """ (id, product_name, product_searched, price, offer_details, discount_percent, emi, rating,comment, customer_name, review_age) values (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"""
            batch = BatchStatement()
            for record in records:
                records_to_insert = tuple(record.values())
                batch.add(insert_record_query, records_to_insert)
            session_.execute(batch)
            return f"rows inserted "
        except Exception as e:
            raise Exception(f"(insertRecords): Something went wrong on inserting multiple records\n" + str(e))

    def findfirstRecord(self, keyspace_name, table_name, query=None):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                print(query)
                rows = session_.execute("SELECT * FROM " + keyspace_name + '.' + table_name + query)
                for row in rows:
                    return row
        except Exception as e:
            raise Exception(f"(findfirstRecord): Failed to find record for the given collection and database\n" + str(e))

    def findAllRecords(self, keyspace_name, table_name):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                rows = session_.execute("SELECT * FROM " + keyspace_name + '.' + table_name)
                return rows
        except Exception as e:
            raise Exception(f"(findAllRecords): Failed to find record for the given collection and database\n" + str(e))

    def findRecordOnQuery(self, keyspace_name, table_name, query):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                rows = session_.execute("SELECT * FROM " + keyspace_name + '.' + table_name + query)
                return rows
        except Exception as e:
            raise Exception(
                f"(findRecordOnQuery): Failed to find record for given query,table or database\n" + str(e))

    def updateOneRecord(self, keyspace_name, table_name, query):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                rows = self.session.execute("UPDATE " + keyspace_name + '.' + table_name + query)
                return rows
        except Exception as e:
            raise Exception(
                f"(updateRecord): Failed to update the records with given table query or database name.\n" + str(
                    e))

    def updateMultipleRecord(self, keyspace_name, table_name, queries):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                batch = BatchStatement()
                for query in queries:
                    batch.add("UPDATE " + keyspace_name + '.' + table_name + query)
                    print(batch)
                session_.execute(batch)
                return f"records updated "
        except Exception as e:
            raise Exception(
                f"(updateMultipleRecord): Failed to update the records with given table query or database name.\n" + str(
                    e))

    def deleteRecord(self, keyspace_name, table_name, query):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                session_.set_keyspace(keyspace_name)
                session_.execute("DELETE FROM " + keyspace_name + '.' + table_name + query)
                return "1 row deleted"
        except Exception as e:
            raise Exception(
                f"(deleteRecord): Failed to delete the record with given table query or database name.\n" + str(
                    e))

    def deleteRecords(self, keyspace_name, table_name, queries):
        """
        """
        try:
            table_check_status = self.isTablePresent(keyspace_name=keyspace_name, table_name=table_name)
            if table_check_status:
                session_ = self.getCassandraSessionObject()
                session_.set_keyspace(keyspace_name)
                batch = BatchStatement()
                for query in queries:
                    batch.add("DELETE FROM " + keyspace_name + '.' + table_name + query)
                session_.execute(batch)
                return "Multiple rows deleted"
        except Exception as e:
            raise Exception(
                f"(deleteRecords): Failed to delete the records with given table query or database name.\n" + str(
                    e))

    def getDataFrameOfCollection(self, keyspace_name, table_name):
        """
        """
        try:
            all_Records = self.findAllRecords(keyspace_name=keyspace_name, table_name=table_name)
            dataframe = pd.DataFrame(all_Records)
            return dataframe
        except Exception as e:
            raise Exception(
                f"(getDataFrameOfCollection): Failed to get DatFrame from provided table and keyspace.\n" + str(e))

    def saveDataFrameIntoCollection(self, keyspace_name, table_name, dataframe):
        """
        """
        try:
            table_check_status = self.isTablePresent(self, keyspace_name, table_name)
            dataframe_dict = dataframe.to_dict('records')
            if table_check_status:
                self.insertRecords(table_name=table_name, keyspace_name=keyspace_name, records=dataframe_dict)
                return "Inserted"
            else:
                self.createTable(table_name=table_name, keyspace_name=keyspace_name)
                self.insertRecords(table_name=table_name, keyspace_name=keyspace_name, records=dataframe_dict)
                return "Inserted"
        except Exception as e:
            raise Exception(
                f"(saveDataFrameIntoCollection): Failed to save dataframe value into collection.\n" + str(e))

    def getResultToDisplayOnBrowser(self, keyspace_name, table_name):
        """
        This function returns the final result to display on browser.
        """
        try:
            response = self.findAllRecords(keyspace_name=keyspace_name, table_name=table_name)
            result = [i for i in response]
            return result
        except Exception as e:
            raise Exception(
                f"(getResultToDisplayOnBrowser) - Something went wrong on getting result from database.\n" + str(e))