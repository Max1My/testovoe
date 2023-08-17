from datetime import datetime
from pprint import pprint
from typing import TypedDict
from uuid import UUID

import psycopg2


class Documents(TypedDict):
    doc_id: UUID
    recived_at: datetime
    document_type: str
    document_data: dict
    processed_at: datetime


class Object(TypedDict):
    object: UUID
    status: int
    level: int
    parent: UUID
    owner: str


class RelatedObject(TypedDict):
    doc_id: UUID
    object_id: UUID


class OperationDetails(TypedDict):
    doc_id: UUID
    operation_data: dict


class Action(TypedDict):
    doc_id: UUID
    object_id: UUID
    new_owner: str
    old_owner: str
    status_new: str
    status_old: str


def connect_to_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="test_migration")
        cursor = connection.cursor()
        return cursor, connection
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def execute(query):
    try:
        cursor, connection = connect_to_db()
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def update_db(query):
    try:
        cursor, connection = connect_to_db()
        cursor.execute(query)
        updated_rows = cursor.rowcount
        connection.commit()
        return updated_rows
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_documents():
    documents = []
    query = "select * from documents"
    records = execute(query)
    for row in records:
        documents.append(Documents(doc_id=row[0],
                                   recived_at=row[1],
                                   document_type=row[2],
                                   document_data=row[3],
                                   processed_at=row[4]))
    return documents


def get_object_ids_from_documents(documents: list):
    objects_ids = []
    for document in documents:
        document_data = document['document_data']
        objects_data = document_data['objects']
        for object in objects_data:
            objects_ids.append(RelatedObject(
                doc_id=document['doc_id'],
                object_id=object
            ))
    return objects_ids


def get_related_objects(objects):
    return [get_details_of_object(object_id['object_id']) for object_id in objects]


def get_details_of_object(object_id):
    query = f"select * from data where object = '{object_id}' and parent is not null"
    records = execute(query)
    for row in records:
        return Object(
            object=row[0],
            status=row[1],
            level=row[2],
            parent=row[3],
            owner=row[4]
        )


def get_operation_details(documents):
    operations_details = []
    for document in documents:
        document_data = document['document_data']
        if any(document_data['operation_details']):
            operations_details.append(OperationDetails(
                doc_id=document.get('doc_id'),
                operation_data=document_data['operation_details']
            ))
    return operations_details


def get_action_list(related_objects, objects_data, operations_details):
    action_list = []
    for obj in related_objects:
        object = obj.get('object')
        for o in objects_data:
            object_id = o.get('object_id')
            if object == object_id:
                document_id = o.get('doc_id')
                for detail in operations_details:
                    if detail.get('doc_id') == document_id:
                        operation_data = detail.get('operation_data')
                        action_list.append(Action(
                            doc_id=document_id,
                            object_id=object_id,
                            new_owner=operation_data.get('owner')['new'] if operation_data.get('owner') else None,
                            old_owner=operation_data.get('owner')['old'] if operation_data.get('owner') else None,
                            status_new=operation_data.get('status')['new'] if operation_data.get('status') else None,
                            status_old=operation_data.get('status')['old'] if operation_data.get('status') else None
                        ))
    return action_list


def clean_actionlist_from_none(action_list):
    result = []
    for action in action_list:
        filtered = {k: v for k, v in action.items() if v is not None}
        action.clear()
        action.update(filtered)
        result.append(action)
    return result


def apply_action(action_list):
    for action in action_list:
        doc_id = action.get('doc_id')
        object_id = action.get('object_id')
        query = f"select status, owner from data where object = '{object_id}'"
        records = execute(query)
        status_new = action.get('status_new')
        status_old = action.get('status_old')
        new_owner = action.get('new_owner')
        old_owner = action.get('old_owner')
        for row in records:
            status_from_db = row[0]
            owner_from_db = row[1]
            if status_old:
                if status_from_db != status_old:
                    update_status(object_id=object_id, new_status=status_new)
            if old_owner:
                if owner_from_db != old_owner:
                    update_owner(object_id=object_id, new_owner=new_owner)
        update_document_processed_at(doc_id)


def update_status(object_id, new_status):
    query = f"UPDATE data SET status = '{new_status}' WHERE object = '{object_id}'"
    update_db(query)


def update_owner(object_id, new_owner):
    query = f"UPDATE data SET owner = '{new_owner}' WHERE object = '{object_id}'"
    update_db(query)


def update_document_processed_at(doc_id):
    date = datetime.now()
    query = f"UPDATE documents SET processed_at = '{date}' WHERE doc_id = '{doc_id}'"
    update_db(query)


if __name__ == '__main__':
    documents = get_documents()
    objects = get_object_ids_from_documents(documents)
    operations_details = get_operation_details(documents)
    related_objects = get_related_objects(objects)
    action_list = get_action_list(related_objects, objects, operations_details)
    action_list = clean_actionlist_from_none(action_list)
    apply_action(action_list)
