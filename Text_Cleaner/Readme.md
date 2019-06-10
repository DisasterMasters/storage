# Cleaning Statuses Text
Removes specified substrings from a given string based on what arguments are passed into the clean function. The order the arguments are entered into the string is the order they are executed in the function. Because of this, certain orders are prefered, as in the 'new_line' argument typically being the most effective when it is done last.

# How To Use
```
from Text_Cleaner import tc
tc_class = tc()

loop{
  returned_string = tc_class.clean( passed_string, ['html_link',  'pic_link', 'dot', 'at_user', 'hashtag'] )
}
```

# PARAMETERS
```
def clean( string, args):

  string: the string to be cleaned
  args: the substrings to be removed
```

# Args Detailed
```
 ['dot', 'at_user', 'hashtag', 'punc', 'html_link', 'pic_link', 'lower', 'numbers', 'stop_words', 'new_line']


'hashtag' in list passed to clean to use
    removes any number of "#'string'" substrings from the passed string

'punc' in list passed to clean to use
    removes any of the follwing characters "  !@#$%^&*()-+=<>?,./:";{}[]\|`~*  " from the passed string

'html_link' in list passed to clean to use
    removes any number of "http:'string'" substrings from the passed string

'pic_link' in list passed to clean to use
    removes any number of "pic'string'" substrings from the passed string

'lower' in list passed to clean to use
    makes the passed string all lowercase

'numbers' in list passed to clean to use
    removes any amount of numbers from the passed string

'stop_words' in list passed to clean to use
    removes any amount of stop_words from the passed string

'new_line' in list passed to clean to use
    removes the "\n" substring and leading and trailing white space from the passed string
```
