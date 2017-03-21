# -*- coding: utf-8 -*-

import config
import luigi
from luigi.format import UTF8
import nltk
from nltk.corpus import stopwords
import praw
from praw.models import MoreComments
import util.common as util

reddit = praw.Reddit(client_id=config.REDDIT_CLIENT_ID,
                     client_secret=config.REDDIT_CLIENT_SECRET,
                     user_agent=config.USER_AGENT)

class GetPoliticsComments(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('data/politics_comments.tsv', format=UTF8)

    def run(self):
        subreddit = reddit.subreddit('politics')
        with self.output().open('w') as out_file:
            for submission in subreddit.hot(limit=10):
                for comment in submission.comments:
                    if isinstance(comment, MoreComments):
                        continue
                    if comment.author == 'AutoModerator':
                        continue
                    text = comment.body.replace("\n", " ") # replace newlines to separate comments
                    out_file.write('{}\n'.format(text))

class GetTheDonaldComments(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('data/the_donald_comments.tsv', format=UTF8)

    def run(self):
        subreddit = reddit.subreddit('the_donald')
        with self.output().open('w') as out_file:
            for submission in subreddit.hot(limit=10):
                for comment in submission.comments:
                    if isinstance(comment, MoreComments):
                        continue
                    if comment.author == 'AutoModerator':
                        continue
                    text = comment.body.replace("\n", " ") # replace newlines to separate comments
                    out_file.write('{}\n'.format(text))

class PreparePoliticsComments(luigi.Task):
    def requires(self):
        return GetPoliticsComments()

    def output(self):
        return luigi.LocalTarget('data/prepared_politics_comments.tsv', format=UTF8)

    def run(self):
        with self.input().open('r') as in_file, self.output().open('w') as out_file:
            for comment in in_file:
                if comment.strip() == '[deleted]':
                    continue
                comment = util.remove_links(comment) # Remove links
                out_file.write('{}'.format(comment))

class PrepareTheDonaldComments(luigi.Task):
    def requires(self):
        return GetTheDonaldComments()

    def output(self):
        return luigi.LocalTarget('data/prepared_the_donald_comments.tsv', format=UTF8)

    def run(self):
        with self.input().open('r') as in_file, self.output().open('w') as out_file:
            for comment in in_file:
                if comment.strip() == '[deleted]':
                    continue
                comment = util.remove_links(comment) # Remove links
                out_file.write('{}'.format(comment))

class ProcessPoliticsComments(luigi.Task):
    def requires(self):
        return PreparePoliticsComments()

    def output(self):
        return luigi.LocalTarget('data/processed_politics_comments.tsv', format=UTF8)

    def run(self):
        with self.input().open('r') as in_file, self.output().open('w') as out_file:
            tokens = []
            for comment in in_file:
                lowers = comment.lower()
                no_punctuation = util.remove_punctuation(lowers)
                tokens.extend(nltk.word_tokenize(no_punctuation))
            filtered = [w for w in tokens if not w in stopwords.words('english')]
            word_list = nltk.FreqDist(filtered)
            for word, frequency in word_list.most_common(50):
                out_file.write('{}\t{}\n'.format(word, frequency))

class ProcessTheDonaldComments(luigi.Task):
    def requires(self):
        return PrepareTheDonaldComments()

    def output(self):
        return luigi.LocalTarget('data/processed_the_donald_comments.tsv', format=UTF8)

    def run(self):
        with self.input().open('r') as in_file, self.output().open('w') as out_file:
            tokens = []
            for comment in in_file:
                lowers = comment.lower()
                no_punctuation = util.remove_punctuation(lowers)
                tokens.extend(nltk.word_tokenize(no_punctuation))
            filtered = [w for w in tokens if not w in stopwords.words('english')]
            word_list = nltk.FreqDist(filtered)
            for word, frequency in word_list.most_common(50):
                out_file.write('{}\t{}\n'.format(word, frequency))

class Process(luigi.Task):
    def requires(self):
        return [ProcessPoliticsComments(), ProcessTheDonaldComments()]

    def output(self):
        return luigi.LocalTarget('done.txt')

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write('done')

if __name__ == '__main__':
    luigi.run()
