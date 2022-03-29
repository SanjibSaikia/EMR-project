import os
from util import get_spark_session
from read import from_files
from process import transform
from write import to_files

def main():

        #ENV Variables
        env=os.environ.get("ENVIRON")
        src_dir=os.environ.get("SRC_DIR")
        file_pattern=f'{os.environ.get("SRC_FILE_PATTERN")}-*'
        src_file_format=os.environ.get("SRC_FILE_FORMAT")
        tgt_dir=os.environ.get("TGT_DIR")
        tgt_file_format=os.environ.get("TGT_FILE_FORMAT")

        #Spark Session
        spark=get_spark_session(env,"SPARK Local Execution")
        
        #Read from files
        read_df=from_files(spark,src_dir,file_pattern,src_file_format)


        #Transformed DF
        transformed_df=transform(read_df) 

        transformed_df.select('created_at','year','month','day').show()

        print(transformed_df.rdd.getNumPartitions())

        #Save files to dir
        to_files(transformed_df,tgt_dir,tgt_file_format)

if __name__=='__main__':
        main()
        
