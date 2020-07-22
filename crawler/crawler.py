import logging
import re
import os
from corpus import Corpus
from lxml import html
from urllib.parse import urljoin,urlparse

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """
    valid_urls = set()
    invalid_urls = []
    trimmed = set()
    subdomains = {}
    most_valid = {
        "url": "",
        "count": 0
    }

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.fetch_url(url)
            current_valid = 0
            for next_link in self.extract_next_links(url_data):
                if self.corpus.get_file_name(next_link) is not None:
                    if self.is_valid(next_link):
                        for subdomain in self.get_subdomains(next_link):
                            if subdomain in self.subdomains:
                                self.subdomains[subdomain] += 1
                            else:
                                self.subdomains[subdomain] = 1
                        current_valid += 1
                        self.valid_urls.add(next_link)
                        self.trimmed.add(self.trim_scheme(next_link))
                        self.frontier.add_url(next_link)
                    else:
                        self.invalid_urls.append(next_link)
            if current_valid > self.most_valid["count"]:
                self.most_valid["count"] = current_valid
                self.most_valid["url"] = url

        #write to file here
        file = open("analytics.txt", "w")
        file.write("Visited Subdomains:\n")
        for subdomain,count in self.subdomains.items():
            file.write(subdomain + ": " + str(count) + "\n")

        file.write("\nUrl with most valid out links: '" + self.most_valid["url"] + "' with " + str(self.most_valid["count"]) + " links.\n\n")

        file.write("Downloaded Urls:\n")
        for downloaded_url in self.valid_urls:
            file.write(downloaded_url + "\n")

        file.write("\nInvalid Urls:\n")
        for invalid_url in self.invalid_urls:
            file.write(invalid_url + "\n")



    def fetch_url(self, url):
        """
        This method, using the given url, finds the corresponding file in the corpus and returns a dictionary
        containing the url, content of the file in binary format, and the content size in bytes. If the url does not
        exist in the corpus, a dictionary with content set to None and size set to 0 is returned.
        """
        try:
            f = open(self.corpus.get_file_name(url), mode='rb')
            content = f.read()
            size = os.path.getsize(self.corpus.get_file_name(url))
        except:
            content = None
            size = 0
        finally:
            url_data = {
                "url": url,
                "content": content,
                "size": size
            }
        return url_data

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method returns a
        list of urls in their absolute form. Validation of links is done later via is_valid method. Duplicated urls will be taken care of by frontier.
        """
        outputLinks = []
        url = url_data["url"]
        content = url_data["content"]
        htmlElem = html.fromstring(content)
        htmlElem.make_links_absolute(url,resolve_base_href=True)
        for e in htmlElem.iterlinks():
            link = e[2]
            if bool(urlparse(link).netloc): #check if absolute
                outputLinks.append(link)
        return outputLinks

    def get_subdomains(self,url):
        final = ["ics.uci.edu"]
        parsed = urlparse(url)
        subdomains = parsed.hostname.split('.')
        for i in range(len(subdomains) - 3):
            if subdomains[i] != 'www':
                stringSD = '.'.join(subdomains[i:])
                final.append(stringSD)
        return final


    def trim_scheme(self, url): #trims http or https from url
        parsed = urlparse(url)
        trimmed = url.replace(parsed.scheme, '')
        return trimmed

    frequency = {
        "url": "",
        "count": 0
    }
    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. Crawler traps are filtered out. Duplicated urls will be taken care of by frontier.
        """
        parsed = urlparse(url)
        path = parsed.path.lower()
        query = parsed.query.lower()

        if parsed.scheme not in set(["http", "https"]):
            return False

        if len(url) > 300:
            return False

        #measuring crawler progress
        base = url.split('?', 1)[0]
        if self.frequency["url"] != base:
            self.frequency["url"] = base
            self.frequency["count"] = 0
        if self.frequency["url"] == base:
            self.frequency["count"] += 1
        if self.frequency["count"] > 500:
            return False

        if "calendar" in query:
            return False

        if self.trim_scheme(url) in self.trimmed: #same url but with http/https
            return False

        parameters = url.split('=') #long hex strings in parameters
        for p in parameters:
            if re.match(r"^[a-zA-Z0-9]{30,}$",p):
                return False

        #repeated directories
        #https://support.archive-it.org/hc/en-us/articles/208332963-Modify-crawl-scope-with-a-Regular-Expression#RepeatingDirectories
        if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", path):
            return False

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False