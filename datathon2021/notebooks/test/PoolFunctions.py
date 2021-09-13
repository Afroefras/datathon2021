from requests import get as get_request
def scrape(url):
    req = get_request(url)
    return url, req.text

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize
sid = SentimentIntensityAnalyzer()
def sentiment(text):
    sentences = sent_tokenize(text)
    polarities = list(map(sid.polarity_scores, sentences))
    return polarities[0]