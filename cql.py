from pyparsing import *
import collections


Term = collections.namedtuple('Term', 'term')
Term.__hash__ = lambda self: hash(self.term)
Term.__str__ = lambda self: str(self.term)
Term.__repr__ = Term.__str__
Triple = collections.namedtuple('Triple', 'term1 relation term2')
Triple.__str__ = lambda self: '%s %s %s' % (str(self.term1), str(self.relation), str(self.term2))
Triple.__repr__ = Triple.__str__
Query = collections.namedtuple('Query', 'prefixes clause sortspec')

charString1 = Regex(r'''[^\(\)\=\<\>\"\/\s]*''')
charString2 = dblQuotedString
comparitorSymbol = Literal('==') | '>' | '<' | '>=' | '<=' | '<>' | '='
identifier = charString1 | charString2
term = identifier | 'and' | 'or' | 'not' | 'prox' | 'sortby'
prefix = uri = modifierName = modifierValue = index = searchTerm = term
term.setParseAction(lambda x: Term(x[0]))
cqlQuery = Forward()
modifier = '/' + modifierName + Optional(comparitorSymbol + modifierValue)
modifierList = OneOrMore(modifier)
namedComparitor = identifier
comparitor = comparitorSymbol | namedComparitor
relation = comparitor + Optional(modifierList)
triple = (index + relation + searchTerm)
triple.setParseAction(lambda x: Triple(*x))
searchClause = (Suppress('(') + cqlQuery + Suppress(')')) | triple | searchTerm
boolean = Literal('and') | 'or' | 'not' | 'prox'
booleanGroup = boolean + Optional(modifierList)
scopedClause = searchClause + ZeroOrMore(booleanGroup + searchClause)
def combine_search_clauses(*x):
    if len(x) == 1: return x[0]
    elif len(x) == 3: return Triple(x[0], x[1], x[2])
    elif len(x) > 3:
        return combine_search_clauses(Triple(x[0], x[1], x[2]), *x[3:])
scopedClause.setParseAction(lambda x: combine_search_clauses(*x))
prefixAssignment = Suppress('>') + prefix + Suppress('=') + uri
prefixAssignment.setParseAction(lambda x: (x[0], x[1]))
prefixAssignments = OneOrMore(prefixAssignment)
prefixAssignments.setParseAction(lambda x: {key:value for (key, value) in x})
cqlQuery << ((prefixAssignments + cqlQuery) | scopedClause)
singleSpec = index + Optional(modifierList)
sortSpec = OneOrMore(singleSpec)
sortedQuery = Forward()
sortedQuery << (Optional(prefixAssignments, default={}) + (scopedClause + Optional(Suppress('sortby') + sortSpec, default=None)))
sortedQuery.setParseAction(lambda x: Query(*x))


def parse_file(filename):
    with open(filename, 'r') as file:
        return parse(file.read())
        
def parse(text):
    return sortedQuery.parseString(text, parseAll=True)[0]


# BNF specification from CQL version 1.2
# http://www.loc.gov/standards/sru/specs/cql.html#bnf
bnf = '''
     sortedQuery :: prefixAssignment sortedQuery
                 =  | scopedClause ['sortby' sortSpec]
        sortSpec :: sortSpec singleSpec | singleSpec
                 =
      singleSpec :: index [modifierList]
                 =
        cqlQuery :: prefixAssignment cqlQuery
                 =  | scopedClause
prefixAssignment :: '>' prefix '=' uri
                 =  | '>' uri
    scopedClause :: scopedClause booleanGroup searchClause
                 =  | searchClause
    booleanGroup :: boolean [modifierList]
                 =
         boolean :: 'and' | 'or' | 'not' | 'prox'
                 =
    searchClause :: '(' cqlQuery ')'
                 =  | index relation searchTerm
                    | searchTerm
        relation :: comparitor [modifierList]
                 =
      comparitor :: comparitorSymbol | namedComparitor
                 =
comparitorSymbol :: '=' | '>' | '<' | '>=' | '<=' | '<>' | '=='
                 =
 namedComparitor :: identifier
                 =
    modifierList :: modifierList modifier | modifier
                 =
        modifier :: '/' modifierName [comparitorSymbol modifierValue]
                 =
    prefix, uri, :: term
   modifierName, =
  modifierValue,
     searchTerm,
           index
            term :: identifier | 'and' | 'or' | 'not' | 'prox' | 'sortby'
                 =
      identifier :: charString1 | charString2
                 =
     charString1 := Any sequence of characters that does not include any of the following:

                        whitespace
                        ( (open parenthesis )
                        ) (close parenthesis)
                        =
                        <
                        >
                        '"' (double quote)
                        /

                    If the final sequence is a reserved word, that token is returned instead. Note that '.'
                    (period) may be included, and a sequence of digits is also permitted. Reserved words are
                    'and', 'or', 'not', and 'prox' (case insensitive). When a reserved word is used in a search
                    term, case is preserved.
     charString2 := Double quotes enclosing a sequence of any characters except double quote (unless preceded
                    by backslash (\)). Backslash escapes the character following it. The resultant value
                    includes all backslash characters except those releasing a double quote (this allows other
                    systems to interpret the backslash character). The surrounding double quotes are not
                    included.
'''
