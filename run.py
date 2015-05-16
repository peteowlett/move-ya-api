import xmltodict
import pandas as pd
import datetime
import os
import sqlalchemy as sqla
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
Base.metadata = MetaData(schema="nhshack")


class ActivityRecords(Base):

    __tablename__ = 'activity_records'
    id = Column(Integer, primary_key=True)
    user = Column(String)
    record_count = Column(Integer)
    record_type = Column(String)
    date_stamp = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    value = Column(Float)

    def __repr__(self):
        return "<Record(user='%s', type='%s')>" % (
                             self.user, self.record_type)


def parse_document(doc):

    transformed_data = pd.DataFrame(columns=['user', 'record_count', 'record_type', 'date_stamp', 'start_date',
                                             'end_date', 'value'])
    obj = xmltodict.parse(doc)
    records = obj['HealthData']['Record']

    for i in range(len(records)):
        rsd = dict(records[i])['@startDate']
        red = dict(records[i])['@endDate']

        record_date_stamp = datetime.datetime(int(red[:4]), int(red[4:6]), int(red[6:8]), 0, 0, 0)
        record_start_date_parsed = datetime.datetime(int(rsd[:4]), int(rsd[4:6]), int(rsd[6:8]),
                                                     int(rsd[8:10]), int(rsd[10:12]), 0)
        record_end_date_parsed = datetime.datetime(int(red[:4]), int(red[4:6]), int(red[6:8]),
                                                   int(red[8:10]), int(red[10:12]), 0)

        try:
            curr_record = [file_path,
                           dict(records[i])['@recordCount'],
                           dict(records[i])['@type'],
                           record_date_stamp,
                           record_start_date_parsed,
                           record_end_date_parsed,
                           float(dict(records[i])['@value'])]
            transformed_data.loc[i] = curr_record
        except:
            pass  # TODO: Yeah this is pretty weak ...

    return transformed_data


def load_document(session, transformed_data):

    for k, v in transformed_data.iterrows():
        new_document = ActivityRecords(user=v.user, record_count=v.record_count, record_type=v.record_type,
                                       date_stamp=v.date_stamp, start_date=v.start_date, end_date=v.end_date,
                                       value=v.value)
        session.add(new_document)
        session.commit()


def retrieve_documents(session):

    retrieved_data = pd.DataFrame(columns=['user', 'record_count', 'record_type', 'date_stamp',
                                           'start_date', 'end_date', 'value'])
    full_results = session.query(ActivityRecords).all()

    for i in range(len(full_results)):
        result = full_results[i]
        curr_record = [result.user, result.record_count, result.record_type, result.date_stamp,
                       result.start_date, result.end_date, result.value]
        retrieved_data.loc[i] = curr_record

    return retrieved_data


def compute_stats(transformed_data):

    # Calculate mean steps per day
    grouped_sum = transformed_data[transformed_data.record_type == 'HKQuantityTypeIdentifierStepCount'].groupby(['user', 'date_stamp']).sum()
    average_daily_steps = grouped_sum.values.sum() / float(len(grouped_sum))

    # Calculate mean distance per day
    grouped_sum = transformed_data[transformed_data.record_type == 'HKQuantityTypeIdentifierDistanceWalkingRunning'].groupby(['user', 'date_stamp']).sum()
    average_daily_distance = grouped_sum.values.sum() / float(len(grouped_sum))

    # Calculate daily average of sum active time (minutes)
    transformed_data['active_time_mins'] = (transformed_data.end_date - transformed_data.start_date).astype('timedelta64[m]')
    transformed_data['active_time_hours'] = transformed_data['active_time_mins'] / 60.0
    transformed_data['value_per_hour'] = transformed_data['value'] / transformed_data['active_time_hours']
    transformed_data_steps = transformed_data[transformed_data.record_type == 'HKQuantityTypeIdentifierStepCount']
    transformed_data_steps_active = transformed_data_steps[transformed_data_steps.value_per_hour > 2000.0]
    grouped_sum = transformed_data_steps_active.groupby(['user', 'date_stamp'])['active_time_mins'].sum()
    average_daily_active_minutes = grouped_sum.values.sum() / float(len(grouped_sum))

    # Calculate daily average of sum inactive time (minutes)
    transformed_data_steps_active = transformed_data_steps_active.sort(['user', 'end_date'])
    transformed_data_steps_active.loc[:, 'end_date_last_active'] = transformed_data_steps_active.shift(1).end_date
    transformed_data_steps_active.loc[:, 'prior_record_user'] = transformed_data_steps_active.shift(1).user
    transformed_data_steps_active.loc[:, 'mins_since_last_active'] = (transformed_data_steps_active.start_date - transformed_data_steps_active.end_date_last_active).astype('timedelta64[m]')
    transformed_data_steps_active.loc[:, 'filtered_mins_since_last_active'] = transformed_data_steps_active.apply(calc_mins_since_last_active, axis=1)
    grouped_sum = transformed_data_steps_active.groupby(['user', 'date_stamp'])['filtered_mins_since_last_active'].sum()
    average_mins_between_activity = grouped_sum.values.sum() / float(len(grouped_sum))

    return_dict = {'avg_daily_steps': float(average_daily_steps),
            'avg_daily_distance': float(average_daily_distance),
            'avg_daily_activity_mins': float(average_daily_active_minutes),
            'avg_daily_inactivity_mins': float(average_mins_between_activity)}

    return return_dict


def calc_mins_since_last_active(s):

    # If prior user different, set 0
    if s.user != s.prior_record_user:
        return 0.0
    # If over 500, return 500 (timeout problem)
    elif s.mins_since_last_active > 500.0:
        return 500.0
    else:
        return s.mins_since_last_active


def main(doc=None):

     # Create the DB engine
    engine = sqla.create_engine(os.environ["DATABASE_URL"])
    Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    if doc is not None:

        # Parse the submitted document
        transformed_data = parse_document(doc)

        # Load the doc into the database
        #load_document(session, transformed_data)

        # Retrieve ALL the results in the DB
        retrieved_data = retrieve_documents(session)

        # Compute the stats
        user_results = compute_stats(transformed_data)
        all_user_results = compute_stats(retrieved_data)

        # Combine and return the results package
        combined_results = {'personal_results': user_results, 'global_results': all_user_results}
        return combined_results

    else:

        # Retrieve ALL the results in the DB
        retrieved_data = retrieve_documents(session)

        # Compute the stats
        all_user_results = compute_stats(retrieved_data)

        # Combine and return the results package
        combined_results = {'personal_results': None, 'global_results': all_user_results}
        return combined_results


if __name__ == '__main__':
    file_path = 'data/pete_export.xml'
    with open(file_path) as fd:
            doc = fd.read()
    print main(doc)
    print main()