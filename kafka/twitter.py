from textblob import TextBlob
from textblob import Word
from nltk.corpus import stopwords
from textblob_fr import PatternTagger, PatternAnalyzer
import re


class Tweet:

    _stopWords: list
    moreBadWord: list = ["u"]
    id: str = ""
    text: str = ""
    cleanText = ""
    polarity: float = 0.0
    subjectivity: float = 0.0
    sentiment:str = "neutral"
    OUTPUT = ["TB_ID", "TB_CLN_TEXT", "TB_POLARITY",
              "TB_SUBJECTIVITY", "TB_SENTIMENT"]
    TRI_POSITIF = 0.1
    TRI_NEGATIF = -0.1

    def __init__(self, tweetID: str, tweetText: str):
        self.id = tweetID
        self.text = tweetText
        self._stopWords = stopwords.words("english")
        self._stopWords.extend(self.moreBadWord)
        self._preprocessing()
        self._predict()

    def _preprocessing(self):
        self.cleanText = self.text.lower()
        self.cleanText = re.sub(
            r'(http|https)://([\w-]+(?:(?:.[\w-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])', '', self.cleanText)
        self.cleanText = re.sub(r'@[A-Za-z0-9]*', '', self.cleanText)
        self.cleanText = re.sub(r'[^A-Za-z0-9]', ' ', self.cleanText)
        

        bbText = TextBlob(self.cleanText)
        self.cleanText = ""
        for word in bbText.words:
            if word not in self._stopWords:
                self.cleanText += word
                self.cleanText += " "

        bbText = TextBlob(self.cleanText)
        bbTokken = bbText.tokens
        self.cleanText = ""
        for tokken in bbTokken:
            bbWord = Word(tokken)
            self.cleanText += bbWord.lemmatize("v")
            self.cleanText += " "

    def _predict(self):
        bbText = TextBlob(self.cleanText)
        self.polarity = bbText.polarity
        self.subjectivity = bbText.subjectivity
        if self.polarity > self.TRI_POSITIF:
            self.sentiment = "positive"
        elif self.polarity < self.TRI_NEGATIF:
            self.sentiment = "negative"
        else:
            self.sentiment = "neutral"

    def output(self):
        return [self.id, self.text ,self.cleanText, self.polarity, self.subjectivity, str(self.sentiment)]
