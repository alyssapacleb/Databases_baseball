import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# Project ID is needed for BigQuery data source, even for local execution.
options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-student',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1 
        }

opts = beam.pipeline.PipelineOptions(flags=[], **options)
p = beam.Pipeline('DataflowRunner', options=opts)

# PTransform: parse line in file, return (actor name, 1)
class CombineDOBFn(beam.DoFn):
        def process(self, element):
            value = element

            #retrieve all columns
            playerID = value.get('playerID')
            nameFirst = value.get('nameFirst')
            nameLast = value.get('nameLast')
            nameGiven = value.get('nameGiven')
            debut = value.get('debut')
            weight = value.get('weight')
            height = value.get('height')
            birthCountry = value.get('birthCountry')
            birthCity = value.get('birthCity')
            birthyear = value.get('birthyear')
            birthmonth = value.get('birthmonth')
            birthday = value.get('birthday')
            bats = value.get('bats')
            throws = value.get('throws')

            #Check to see if any of the date of birth values are null so we can foce them into a dummy value
            if birthyear == None:
                dob = '0001-01-01'
                return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]

            if birthyear == None:
                dob = '0001-01-01'
                return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]

            if birthday == None:
                dob = '0001-01-01'
                return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]

            #renurn a tuple/dictionary with the primary key as the key. 
            dob = str(birthyear) + '-' + str(birthmonth) + '-' + str(birthday)
            return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]


# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
    def process(self, element):
            playerID, obj = element # obj is an _UnwindowedValues type
            
            # Convert the unwindowed value into a list so I can grab the values
            val = list(obj)
            
            # For some reason, it is a nested list so I need to retrieve the inner list 
            val = val[0]
            
            # Retrieved the columns from the list
            nameFirst = val[0]
            nameLast = val[1]
            nameGiven = val[2]
            debut = val[3]
            weight = val[4]
            height = val[5]
            birthCountry = val[6]
            birthCity = val[7]
            dob = val[8]
            bats = val[9]
            throws = val[10]
            
            # Made the BQ record with this dictionary within a list.
            record = {'playerID': playerID, 'nameFirst': nameFirst, 'nameLast':nameLast, 'nameGiven':nameGiven,'debut':debut,'weight':weight, 'height':height, 'birthCountry':birthCountry, 'birthCity':birthCity, 'dob':dob, 'bats':bats, 'throws':throws}
            
            return [record] 


# Get the BQ file I want to manipulate
sql = 'SELECT * FROM seanlahman_modeled.players2'

bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)


# write PCollection to log file
query_results | 'Write to log 1' >> WriteToText(DIR_PATH + 'input.txt')

# apply a ParDo to the PCollection 
t1_pcoll = query_results | 'Extract Players' >> beam.ParDo(CombineDOBFn())

# apply GroupByKey to the PCollection
intermediate_pcoll = t1_pcoll | 'Group by players' >> beam.GroupByKey()

# write PCollection to a file 
intermediate_pcoll | 'Write File Intermediately' >> WriteToText(DIR_PATH + 'unwindowed.txt')

#nipulate the file to send to BQ
done = intermediate_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

# Make the clean data a txt file
done | 'Write File' >> WriteToText(DIR_PATH + 'output.txt')
    
# make the BQ table
qualified_table_name = PROJECT_ID + ':seanlahman_modeled.players2_Beam_DF'
    
dataset_id = 'seanlahman_modeled'
table_id = 'players2_Beam_DF'
schema_id = 'playerID:STRING, nameFirst:STRING, nameLast:STRING, nameGiven:STRING, debut:DATE, weight:INTEGER, height:INTEGER, birthCountry:STRING, birthCity:STRING, dob:DATE, bats:STRING, throws:STRING'
        
done | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(dataset = dataset_id, table = table_id, schema = schema_id, project = PROJECT_ID, create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE))
        
                
result = p.run()
result.wait_until_finish()




