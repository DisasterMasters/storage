import re
import difflib
import nltk
from nltk import *
from nltk.corpus import *

#  How to use

#  from Text_Cleaner import tc
#  tc_class = tc()
#  returned_string = tc_class.clean( passed_string, ['html_link',  'pic_link', 'dot', 'at_user', 'hashtag', 'punc', 'lower', 'numbers'] )

class tc:
     
    #initialize the class
    def __init__(self):
        self.arg_list = ['dot', 'at_user', 'hashtag', 'punc', 'html_link', 'pic_link', 'lower', 'numbers', 'stop_words', 'new_line', 'english', 'retweet', 'via']       
        self.flag = 0
        self.stop_words = set(stopwords.words('english')) 
      
    # Parameters: string -- this is the string you want cleaned
    #            args -- this is a list of strings of all the things you want removed from the passed string,
    #                    the list is filled with strings from the arg_list above
    # Returns: string -- the cleaned string
    def clean( self, string, args):  
        self.flag = 0
        for arg in args:
            if arg not in self.arg_list:
                best = difflib.get_close_matches(arg, self.arg_list)
                raise Exception('Error: "' + arg + '" not a cleaning method, did you mean "' + best[0] + '"\n' + 
                                "Possible methods ['dot', 'at_user', 'hashtag', 'punc', 'html_link','pic_link', 'lower', 'numbers', 'stop_words', 'new_line', 'english', 'retweet', 'via']")
                self.flag = 1
        if self.flag == 1:
            return None
        
        for arg in args:
            if arg == 'dot':
                string = self.remove_dot(string)
                
            elif arg == 'at_user':
                string = self.remove_at_user(string)
                
            elif arg == 'hashtag':
                string = self.remove_hashtag(string)
                
            elif arg == 'punc':
                string = self.remove_punc(string)
                
            elif arg == 'html_link':
                string = self.remove_html_link(string)
                
            elif arg == 'pic_link':
                string = self.remove_pic_link(string)
            
            elif arg == 'lower':
                string = self.make_lowercase(string)
            
            elif arg == 'numbers':
                string = self.remove_numbers(string)
                
            elif arg == 'stop_words':
                string = self.remove_stop_words(string)
                
            elif arg == 'new_line':
                string = self.remove_new_line(string)
                
            elif arg == 'english':
                string = self.remove_not_english(string)
                
            elif arg == 'retweet':
                string = self.remove_retweets(string)
                
            elif arg == 'via':
                string = self.remove_via(string)
                
        return string
    
    #add 'dot' in list passed to clean to use
    #removes the ". " substring at the begining of the passed string
    def remove_dot( self, string ):
        remove = re.findall("\A\. *", string)
        for i in remove:
            string = string.replace(i, '')
        return string
    
    #add 'at_user' in list passed to clean to use
    #removes any number of "@'string'" substrings from the passed string 
    def remove_at_user( self, string ):
        remove = re.findall(r"\@\w+ *", string)
        for i in remove:
            string = string.replace(i, '')
        return string
    
    #add 'hashtag' in list passed to clean to use
    #removes any number of "#'string'" substrings from the passed string
    def remove_hashtag( self, string ):
        remove = re.findall(r"\#\w+ *", string)
        for i in remove:
            string = string.replace(i, '')
        return string
    
    #add 'punc' in list passed to clean to use
    #removes any of the follwing characters "  !@#$%^&*()-+=<>?,./:";{}[]\|`~*  " from the passed string
    def remove_punc( self, string ):
        string = re.sub(r'[^\w\s]','',string)
        return string
    
    #add 'html_link' in list passed to clean to use
    #removes any number of "http:'string'" substrings from the passed string
    def remove_html_link( self, string ):
        remove = re.findall(r"http://" + ".+xa0", string)
        remove1 = re.findall(r"https://" + ".+xa0", string)
        for i in remove:
            string = string.replace(i, '')
        for i in remove1:
            string = string.replace(i, '')
        return string
    
    #add 'pic_link' in list passed to clean to use
    #removes any number of "pic'string'" substrings from the passed string
    def remove_pic_link( self, string ):
        remove = re.findall(r"pic\S+", string)
        for i in remove:
            string = string.replace(i, '')
        return string
    
    #add 'lower' in list passed to clean to use
    #makes the passed string all lowercase
    def make_lowercase( self, string ):
        return string.lower()
    
    #add 'numbers' in list passed to clean to use
    #removes any amount of numbers from the passed string
    def remove_numbers( self, string ):
        string = re.sub(r'\d+', '', string)
        return string
    
    #add 'stop_words' in list passed to clean to use
    #removes any amount of stop_words from the passed string
    def remove_stop_words( self, string ):
        for word in self.stop_words: 
            string = re.sub(r" " + re.escape(word) + r" ", " ", string)
        return string    
    
    #add 'new_line' in list passed to clean to use
    #removes the "\n" substring and leading and trailing white space from the passed string
    def remove_new_line( self, string ):
        string = string.replace('\n', '')
        string = string.rstrip()
        string = string.lstrip()
        return string
    
    #detects if string is english and returns what language it is
    def detect_language( self, input_val ):
        lang_ratio = {}
        tokens = wordpunct_tokenize(input_val)
        words = [word.lower() for word in tokens]
        for language in stopwords.fileids():
            stopwords_set = set(stopwords.words(language))
            words_set = set(words)
            common_elements = words_set.intersection(stopwords_set)
            lang_ratio[language] = len(common_elements)
        ratios = lang_ratio
        language = max(ratios, key=ratios.get)
        return language
    
    #add 'english' in list passed to clean to use
    #makes any string return "" if not in english
    def remove_not_english( self, string ):
        if self.detect_language(string) != 'english':
            string = ""
        return string
    
    #add 'retweet' in list passed to clean to use
    #makes any string return "" if a retweet
    def remove_retweets( self, string ):
        remove = re.findall(r"RT \@\w+ *", string)
        if len(remove) > 0:
            string = ""
        return string
    
    #add 'via' in list passed to clean to use
    #removes via from string passed
    def remove_via( self, string ):
        remove = re.findall(r"\s*via *", string)
        for i in remove:
            string = string.replace(i, ' ')
        return string