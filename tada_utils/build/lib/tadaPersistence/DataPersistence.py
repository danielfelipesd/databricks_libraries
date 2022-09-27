def persistDataFrame(df,saveFomat,savemode,savePath,dataFrameCoalesce=1,partition=None):
    if partition != None:
        if len(partition) == 1:
            df.coalesce(dataFrameCoalesce)\
                .write.format(saveFomat)\
                .mode(savemode)\
                .partitionBy(partition[0])\
                .save(savePath)
        elif len(partition) == 2:
            df.coalesce(dataFrameCoalesce)\
                .write.format(saveFomat)\
                .mode(savemode)\
                .partitionBy(partition[0],partition[1])\
                .save(savePath)
        elif len(partition) == 3:
            df.coalesce(dataFrameCoalesce)\
                .write.format(saveFomat)\
                .mode(savemode)\
                .partitionBy(partition[0],partition[1],partition[2])\
                .save(savePath)
    else:
        df.coalesce(dataFrameCoalesce)\
            .write.format(saveFomat)\
            .mode(savemode)\
            .save(savePath)