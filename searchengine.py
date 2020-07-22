from bs4 import BeautifulSoup
from collections import defaultdict
from pymongo import MongoClient
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import lxml
import math
import json
import nltk
import re
nltk.download('stopwords')


client = MongoClient("mongodb://localhost:27017/")
database = client.index
collection = database.tokens


class SearchEngine:

    def __init__(self, file):
        self.file = file
        self.stopwords = set(stopwords.words('english'))
        self.index = defaultdict(lambda: defaultdict(int))
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.stemmer = PorterStemmer()
        self.totalDocs = 0
        self.results = defaultdict(float)

    def get_files(self):
        try:
            with open(self.file,encoding='utf-8') as json_file:
                data = json.load(json_file)
                for key in data:
                    if key == "39/373":
                        continue
                    if len(data[key]) <= 300:
                        print(key)
                        self.find_text(key,data[key])
        except Exception as e:
            print(e)

    def create_tokens(self,words):
        tokens = []
        for word in words:
            word = word.lower()
            if word not in self.stopwords and len(word) <= 40 and len(word) > 1 and (re.match("^[a-z]+$",word) or re.match("^[0-9]+$",word)):
                tokens.append(self.stemmer.stem(word))
        return tokens

    def find_text(self,path,url):
        frequencies = defaultdict(int)
        self.totalDocs += 1
        soup = BeautifulSoup(open("WEBPAGES_RAW/"+path), "lxml")
        headers = [] #tokenized list
        for headerWords in soup.find_all(['h1','h2','h3','b','strong']):
            headers += self.create_tokens(self.tokenizer.tokenize(headerWords.text))


        content = [] #TOTAL VISIBLE CONTENT ON PAGE, tokenized list
        for b in soup.find_all('body'):
            content += self.create_tokens(self.tokenizer.tokenize(b.text))


        #adding to index...
        #form is word: url: tfidf*weight

        for c in content:
            if c in headers: #more weight
                self.index[c][url] = 1.5
            else:
                self.index[c][url] = 1.0
            frequencies[c] += 1



        #adding term frequency
        total = len(content)
        for (word,frequency) in frequencies.items():
            entry = self.index[word][url]
            self.index[word][url] = (frequency / total) * entry



    def query(self,query):
        query = self.create_tokens(self.tokenizer.tokenize(query))
        return query

    def insertIDF(self):
        for (word, urls) in self.index.items():
            for (url, docinfo) in urls.items():
                entry = docinfo
                tfidf = entry * math.log10((self.totalDocs + .001)/(len(urls) + .001)) #accounts for dividing by 0
                self.index[word][url] = tfidf

    def insertDB(self):
        for x in self.index:
            collection.insert({"token": str(x), "value": self.index[x]}, check_keys=False)


    def run(self):
        self.get_files() #run on all files in corpus here
        self.insertIDF()
        print("length: ", len(self.index))
        print("numbers of documents: ", self.totalDocs)
        collection.remove()  # Clears the existing database before creation
        self.insertDB()  # Inserts index in to db

    def search(self,query):
        #while True:             #Retrieves Query from DB
        self.results = defaultdict(float)
        #tempInput = str(input("Input Query: "))
        tempInput = query
        if tempInput == "quit":
            #break
            return
        query = self.query(tempInput)
        if len(query) > 1:
            for q in query:
                temp = {"token": q}
                found = collection.find_one(temp)
                try:
                    values = found["value"]
                    for v,k in values.items(): #v is url, k is tfidf
                        if v not in self.results:
                            self.results[v] = k
                        else: #multiple words in same document
                            self.results[v] += k
                except Exception as e:
                    continue
        else:
            temp = {"token": query[0]}
            found = collection.find_one(temp)
            try:
                values = found["value"]
                self.results.update(values)
            except Exception as e:
                #continue
                return


        '''print("Showing 20 results out of ", len(self.results))
        for key,value in sorted(self.results.items(),key=lambda item: item[1],reverse=True)[:20]:
            print(key)
        print("\n")'''



if __name__ == "__main__":
    engine = SearchEngine("WEBPAGES_RAW/bookkeeping.json")
    #engine.run()
    engine.search()