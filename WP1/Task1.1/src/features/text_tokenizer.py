
import re
import string
from collections import OrderedDict

import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem.porter import PorterStemmer

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class TextTokenizer(object):
    """
    Class for tokenizing input text, based on Spacy.
    """

    _stopwords = set(stopwords.words('english'))
    _punct_map = str.maketrans('', '', string.punctuation)
    _stemmer = PorterStemmer()
    _spacy_en = spacy.load('en')

    def __init__(
            self, stop=True, punct=True, lower=True, stem=True,
            replace_nums=False
    ):
        """
        :param stop: Remove stopwords? (True = remove, False = do not remove)
        :param punct: Remove punctuation? (True = remove, False = do not remove)
        :param lower: Covert words to lowercase? (True = convert)
        :param stem: Stem words? (True = stem, False = not stem)
        :param replace_nums: Replace numbers with a token? (True = replace)
        """
        self._stop = stop
        self._stem = stem
        self._lower = lower
        self._punct = punct
        self._replace_nums = replace_nums

    @staticmethod
    def is_num(t: str) -> bool:
        """Check if string contains only digits
        
        :param t: input string
        :type t: str
        :return: true if t contains only digits, false otherwise
        :rtype: bool
        """
        return all(c.isdigit() for c in t)

    def tokenize(self, text):
        """
        :param text: text to be processed
        :return: list of tokens
        """
        text = text.strip()
        # \x00 -- speacial character appearing in PDF files, represents NULL
        text = text.replace('\x00', ' ')
        # replace floating point numbers with a single token
        if self._replace_nums:
            text = re.sub(r"\d+\.\d+", ' floattoken ', text)
        tokens = []
        for tok in TextTokenizer._spacy_en.tokenizer(text):
            is_stop = tok.is_stop
            if self._stop and is_stop:
                continue
            token = tok.lower_ if self._lower else tok.text
            if self._stem:
                token = TextTokenizer._stemmer.stem(token)
            if len(token):
                tokens.append(token)
        if self._replace_nums:
            tokens = ['inttoken' if self.is_num(t) else t for t in tokens]
        if self._punct:
            tokens = [t.translate(TextTokenizer._punct_map) for t in tokens]
        tokens = [t.strip() for t in tokens]
        tokens = [t for t in tokens if len(t)]
        return tokens
