
from sklearn.metrics import confusion_matrix, accuracy_score

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class Metrics(object):

    @staticmethod
    def metrics(pred, y):
        """
        :param pred:
        :param y:
        :return:
        """
        tn, fp, fn, tp = confusion_matrix(y, pred).ravel()
        return {
            'n_samples': len(y),
            'correct': int(tn + tp),
            'accuracy': round(100 * accuracy_score(y, pred), 4),
            'tn': int(tn),
            'tp': int(tp),
            'fn': int(fn),
            'fp': int(fp)
        }
