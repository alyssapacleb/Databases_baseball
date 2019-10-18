import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

PROJECT_ID = os.environ['PROJECT_ID']


# Project ID is needed for BigQuery data source, even for local execution.
options = {'project': PROJECT_ID}

opts = beam.pipeline.PipelineOptions(flags=[], **options)


# PTransform: parse line in file, return (actor name, 1)
class CleanCountriesFn(beam.DoFn):
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
        
        #clean up the birthCountry column using the distinct countries. 
        if birthCountry == 'USA':
            birthCountry = 'United States of America'
        if birthCountry == 'D.R.':
            birthCountry = 'Dominican Republic'
        if birthCountry == 'CAN':
            birthCountry = 'Canada'
        if birthCountry == 'P.R.':
            birthCountry = 'Puerto Rico'
        if birthCountry == 'V.I.':
            birthCountry = 'U.S. Virgin Islands'
        
        #return a tuple/dictionary with the primary key as the key. 
        return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, birthyear, birthmonth, birthday, bats, throws])]

class CombineDOBFn(beam.DoFn):
    def process(self, element):
        playerID, obj = element
        
        val = list(obj)
        
        val = val[0]
        
        # retrieve all valuews
        nameFirst = val[0]
        nameLast = val[1]
        nameGiven = val[2]
        debut = val[3]
        weight = val[4]
        height = val[5]
        birthCountry = val[6]
        birthCity = val[7]
        birthyear = val[8]
        birthmonth = val[9]
        birthday = val[10]
        bats = val[11]
        throws = val[12]

        #Check to see if any of the date of birth values are null so we can foce them into a dummy value
        if birthyear == None:
            dob = '0001-01-01'
            return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]
        
        if birthmonth == None:
            dob = '0001-01-01'
            return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]
        
        if birthday == None:
            dob = '0001-01-01'
            return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]
            
        dob = str(birthyear) + '-' + str(birthmonth) + '-' + str(birthday)
        return [(playerID, [nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, dob, bats, throws])]



# PTransform: format for BQ sink
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        playerID, obj = element # obj is an _UnwindowedValues type
        
        # Convert the unwindowed value into a list so I can grab the values
        val = list(obj)
        print(val)
        # For some reason, it is a nested list so I need to retrieve the inner list 
        
        
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


# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner', options = opts) as p:
    
    # Get the BQ file I want to manipulate
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM seanlahman_modeled.players'))
    
    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection 
    t1_pcoll = query_results | 'Extract Players' >> beam.ParDo(CleanCountriesFn())

    # apply GroupByKey to the PCollection
    intermediate_pcoll = t1_pcoll | 'Group by players' >> beam.GroupByKey()

    # write PCollection to a file 
    intermediate_pcoll | 'Write File Intermediately' >> WriteToText('unwindowed.txt')
    
    # second transform
    t2_pcoll = intermediate_pcoll | '2nd transform' >> beam.ParDo(CombineDOBFn())
    
    # write to a file to debug
    t2_pcoll | "write file t2" >> WriteToText('t2.txt')

    # Manipulate the file to send to BQ
    done = t2_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    # Make the clean data a txt file
    done | 'Write File' >> WriteToText('output.txt')
    
    # make the BQ table
    qualified_table_name = PROJECT_ID + ':seanlahman_modeled.players_Beam'
    
    table_schema = 'playerID:STRING, nameFirst:STRING, nameLast:STRING, nameGiven:STRING, debut:DATE, weight:INTEGER, height:INTEGER, birthCountry:STRING, birthCity:STRING, dob:DATE, bats:STRING, throws:STRING'
    
    done | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, schema = table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
