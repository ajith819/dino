import datetime
def get_datetime():
    dt1 = datetime.datetime.now()
    return dt1.strftime("%d %B, %Y")
monthstr = get_datetime()
print(monthstr)
urlapi= '/data/wiki/url'
ERRORNOTIFICATIONARN = '/data/errorarn'
SUCCESSNOTIFICATIONARN='/data/successarn'
COMPONENT_NAME = 'DE_WIKI_DATA_EXTRACT'
ERROR_MSG = f'NEED ATTENTION ****API ERROR /KEY EXPIRED ** ON {monthstr} **'
SUCCESS_MSG = f'SUCCESSFULLY EXTRACTED WIKI FILES FOR {monthstr}*'
SUCCESS_DESCRIPTION='SUCCESS'
ENVIRONMENT = '/data/environment'