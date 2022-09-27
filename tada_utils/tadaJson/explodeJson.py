import re

class ExplodeJson:

    def __init__(self, config):
        self.partition = config.__getitem__('partition')
        self.partitionReferenceColumn = config.__getitem__('partitionReferenceColumn')
        self.idTable = config.__getitem__('idTable')
        self.principalTableName = config.__getitem__('principalTableName')
        self.listTable = []
        self.listRefineTables = []
        self._regex = '[^a-zA-Z0-9_-]'

    def recursiveParseJson(self,_json):
        def parseJson(_json,listJson=[]):
            try:
                index = 0
                items = []
                if len(listJson) == 0:
                    for item in _json:
                        for k, v in item.items():
                            if type(v) == list or type(v) == dict:
                                if self.partition != "" and self.partitionReferenceColumn != "":
                                    items.append((item[self.idTable],index,re.sub(self._regex, '', k),v,item[self.partitionReferenceColumn]))
                                else:
                                    items.append((item[self.idTable],index,re.sub(self._regex, '', k),v))

                        if self.partition != "" and self.partitionReferenceColumn != "":
                            item[self.partition] = item[self.partitionReferenceColumn]

                        index = index + 1

                    for it in items:
                        _json[it[1]].pop(it[2])

                    self.listTable.append((self.principalTableName,_json))

                    if len(items) > 0:
                        parseJson(None,items)
                else:
                    for lj in listJson:
                        j = lj[3]
                        nj = {}
                        if type(j) == dict:
                            j[self.idTable] = lj[0]
                            for k, v in j.items():
                                if type(v) == list or type(v) == dict:
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                else:
                                    nj[re.sub(self._regex, '', k)]=v
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                            nj[self.partition] = lj[4]

                            self.listTable.append((lj[2],nj))

                        elif type(j) == list:
                            for item in j:
                                nj = {}
                                try:
                                    for k,v in item.items():
                                        if type(v) == list or type(v) == dict:
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                        else:
                                            nj[re.sub(self._regex, '', k)]=v
                                            nj[self.idTable] = lj[0]
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]

                                    self.listTable.append((lj[2],nj))
                                except AttributeError as error:
                                    _str = ""
                                    for item in lj[3]:
                                        _str = _str+str(item)+"|"

                                    nj[self.idTable] = lj[0]
                                    nj[lj[2]] = _str[:len(_str)-1]
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2],nj))

                if len(items) > 0:
                    parseJson(None,items)
            except Exception as e:
                strError = 'Error in parseJson(): '+str(e)
                strError = strError.replace("'","")
                raise Exception(strError)

        parseJson(_json)

    def refineTables(self):
        try:
            listaTB = self.listTable
            listNmTb = []

            for l in listaTB:
                if type(l[1]) == list:
                    self.listRefineTables.append((l))

            for tb in self.listRefineTables:
                listaTB.remove(tb) 

            for tb in listaTB:
                listNmTb.append(tb[0])

            listNmTb = list(dict.fromkeys(listNmTb))

            for nmTb in listNmTb:
                tb=[]
                for ltb in listaTB:
                    if ltb[0] == nmTb:
                        tb.append(ltb[1])
                self.listRefineTables.append((nmTb,tb))
        except Exception as e:
            strError = 'Error in refineTables(): '+str(e)
            strError = strError.replace("'","")
            raise Exception(strError)