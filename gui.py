import tkinter as tk
import searchengine as SearchEngine

class SearchEngineGUI:

    def __init__(self):
        self.engine = SearchEngine.SearchEngine("WEBPAGES_RAW/bookkeeping.json")
        self.master = tk.Tk()
        self.master.geometry("700x500")
        self.master.title("Search Engine")
        tk.Label(self.master,
                 text="Enter a query:").grid(row=0)
        self.entry = tk.Entry(self.master)
        self.entry.grid(row=1)
        self.button = tk.Button(self.master, text = 'Search',command = self.search).grid(row=2)

        self.master.grid_columnconfigure(0, weight=1)

    def search(self):
        query = self.entry.get()
        self.engine.search(query)
        results = sorted(self.engine.results.items(),key=lambda item: item[1],reverse=True)[:20]
        text = tk.Text(self.master)
        text.insert(tk.END,"Showing 20 results of "+str(len(self.engine.results))+':\n')
        for key,value in results:
            text.insert(tk.END,key+'\n')
        text.grid(row=3)


    def run(self):
        tk.mainloop()


if __name__ == '__main__':
    gui = SearchEngineGUI()
    gui.run()