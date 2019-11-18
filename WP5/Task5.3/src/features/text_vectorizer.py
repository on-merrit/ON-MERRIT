
from typing import List, Dict, Any

from sklearn.feature_extraction.text import CountVectorizer

from src.features.text_tokenizer import TextTokenizer

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class TextVectorizer(object):

    def __init__(
            self, tokenizer_cfg: Dict[str, Any], vectorizer_cfg: Dict[str, Any]
    ) -> None:
        """Initialize class. Saves config to be later used when doing
        vectorization.
        
        :param tokenizer_cfg: config to the passed to the text tokenizer
        :type tokenizer_cfg: Dict[str, Any]
        :param vectorizer_cfg: config to be passed to the text vectorizer
        :type vectorizer_cfg: Dict[str, Any]
        :return: None
        :rtype: None
        """
        self._tokenizer_cfg = tokenizer_cfg
        self._vectorizer_cfg = vectorizer_cfg

    def vectorize(
            self, train_data: List[str], test_data: List[str]
    ) -> 'np.array[np.int64]':
        """Tokenize input corpus
        
        :param train_data: list of training examples
        :type train_data: List[str]
        :param test_data: list of test examples
        :type test_data: List[str]
        :return: numpy array of shape [number of samples, number of features]
        :rtype: np.array[np.int64]
        """
        tt = TextTokenizer(**self._tokenizer_cfg)
        cv = CountVectorizer(tokenizer=tt.tokenize, **self._vectorizer_cfg)
        cv.fit(train_data)
        return cv.transform(train_data), cv.transform(test_data)
