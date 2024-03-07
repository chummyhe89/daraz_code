"""
Author: Bilal Tariq (175453)
Date: December 17, 2023
Description: The script is designed to retrieve responses from the Qualtrics API. If you are involved in the development
of this script, ensure its compatibility with both Windows and Linux environments.
"""

import numpy as np
import os
import requests as r
import zipfile
import io
import json
import pandas as pd
from datetime import date, datetime, timedelta
import pytz
from dateutil.parser import parse
import warnings
import logging

''' Path Management'''
SLSH = os.path.sep
rootPath = os.getcwd()
#rootPath = 'DATA' + SLSH + 'FeedDynamix' + SLSH + 'DataFeeds' + SLSH + 'Qualtrics'
reponsesPath = rootPath + SLSH + 'Responses'
logPath = rootPath + SLSH + 'Logs'
''' Path Management'''

''' Log File.'''
today_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = "logs_" + today_datetime + ".log"
logPath = logPath + SLSH + log_filename
print(logPath)
logging.basicConfig(filename=logPath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Root Path ' + str(rootPath))
logging.info('Execution started. ' + str(datetime.now()))
logging.info('Log file created at: ' + str(datetime.now()))
logging.info('Path of log file: ' + str(logPath))
''' Log File.'''

'''Directory where CSV will be saved.'''
today_date = datetime.now().strftime("%Y%m%d")
responses_filename = "QualtricsResponses_" + today_date + ".csv"
logging.info('Responses filename: ' + str(responses_filename))
responses_filepath = reponsesPath + SLSH + responses_filename
logging.info('Responses file path: ' + str(responses_filepath))
print(responses_filepath)
'''Directory where CSV will be saved.'''


class Parser(object):

    def __init__(self):
        return

    def extract_values(self, obj=None, key=None):
        ''' This outer method  a specific value from a nested dictionary.

        :param obj: a dictionary object.
        :param key: The Specific Key that the value is associated with.
        :return: the value associated with the given key
        '''
        values = []

        def extract(obj, values, key):
            '''This inner method recursively searches for the given key in a nested dictionary. (Not a User-Facing Method)

            :param obj: a dictionary object.
            :param values: a list that will house the values of the given key.
            :param key: The Specific Key that the value is associated with.
            :return: the value associated with the given key
            '''
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, values, key)
                    elif k == key:
                        values.append(v)
            elif isinstance(obj, list):
                for j in obj:
                    extract(j, values, key)
            return values

        results = extract(obj, values, key)
        return results

    def extract_keys(self, obj):
        '''This outer method extracts all of the keys from a nested dictionary.

        :param obj: a dictionary object.
        :return: a list of keys within a given dictonary.
        '''
        keys = []

        def extract(obj, keys):
            '''This inner method recursively locates each of the keys within a nested dictionary.

            :param obj: a dictionary object.
            :param keys: a list of previously identified keys
            :return: a list of keys within a given recursion of the inner method.
            '''
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        keys.append(k)
                        extract(v, keys)
                    else:
                        keys.append(k)
            elif isinstance(obj, list):
                for element in obj:
                    extract(element, keys)
            return keys

        obj_keys = extract(obj, keys)
        return obj_keys

    def json_parser(self, response=None, keys=[], arr=True):
        '''This method itterates over the all of the keys within a keys list.'''

        # Improvement: Include the Extract keys method in this method.
        elements = [self.extract_values(response, item) for item in keys]
        if arr == True:
            return np.array(elements).T
        else:
            return elements


class Qualtrics400Error(Exception):
    '''This Exception handles errors associated with the HTTP 400 (Bad Request) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Qualtrics401Error(Exception):
    '''This Exception handles errors associated with the HTTP 401 (Unauthorized) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Qualtrics403Error(Exception):
    '''This Exception handles errors associated with the HTTP 403 (Forbidden) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Qualtrics429Error(Exception):
    '''This Exception handles errors associated with the HTTP 429 (Too Many Requests) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Qualtrics500Error(Exception):
    '''This Exception handles errors associated with the HTTP 500 (Internal Server Error) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Qualtrics503Error(Exception):
    '''This Exception handles errors associated with the HTTP 503 (Temporary Internal Server Error) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Qualtrics504Error(Exception):
    '''This Exception handles errors associated with the HTTP 504 (Gateway Timeout) responses.'''

    def __init__(self, msg):
        super().__init__(msg)


class Credentials(object):
    ''' This class handles the setup of credentials needed to setup the Qualtrics API Authorization. Use the
    qualtrics_api_credentials method to create enviornment variables that will automatically populate the correct
    HTTP headers for the request that you are making. '''

    def __init__(self, token, url):
        pass

    def qualtrics_api_credentialsV1(self, token, data_center, directory_id=None):
        pass

    def header_setup(self, content_type=False, xm=True, path=None):
        '''This method accepts the argument content_type and returns the correct header
        , and base url. (Not a User-Facing Method)

        :param content_type: use to return json response.
        :return: a HTML header and base url.
        '''
        if xm:
            assert os.environ[
                'directory_id'], 'Hey there! This endpoint is only accessible for XM Directory Users . If you have ' \
                                 'access to the XM Directory, then be sure to include your directory_id when you use ' \
                                 'the qualtrics_api_credentials() method. '
            path = 'directories/{0}/'.format(os.environ['directory_id']) if xm else path

        header = {"x-api-token": "e5rwP4pMqhrqlYdNDWfUJRYjpmVl6W0yeNaLeR8K"}
        base_url = "http://syd1.qualtrics.com/API/v3/" + path
        if content_type is True:
            header["Content-Type"] = "application/json"
        return header, base_url


class Responses(Credentials):
    '''This is a child class to the credentials class that gathers the survey responses from Qualtrics surveys'''

    def __init__(self):
        return

    def setup_request(self, file_format='csv', survey=None):
        ''' This method sets up the request and handles the setup of the request for the survey.'''

        assert survey != None, 'Hey There! The survey parameter cannot be None. You need to pass in a survey ID as a ' \
                               'string into the survey parameter.'
        assert isinstance(survey, str) == True, 'Hey There! The survey parameter must be of type string.'
        assert len(
            survey) == 18, 'Hey there! It looks like your survey ID is a the incorrect length. It needs ' \
                           'to be 18 characters long. Please try again.'
        assert survey[
               :3] == 'SV_', 'Hey there! It looks like your survey ID is incorrect. You can find the survey ID on ' \
                             'the Qualtrics site under your account settings. Please try again.'

        headers, url = self.header_setup(content_type=True, xm=False, path='responseexports/')
        payload = {"format": file_format, "surveyId": survey}
        request = r.request("POST", url, data=json.dumps(payload), headers=headers)
        response = request.json()
        try:
            progress_id = response['result']['id']
            return progress_id, url, headers
        except:
            logging.info("ServerError:Error Code: " + response['meta']['error']['errorCode'])
            logging.info("Error Message: " + response['meta']['error']['errorMessage'])

    def send_request(self, file_format='csv', survey=None):
        '''This method sends the request, and sets up the download request.'''
        file = None
        progress_id, url, headers = self.setup_request(file_format=file_format, survey=survey)
        check_progress = 0
        progress_status = "in progress"
        while check_progress < 100 and (progress_status != "complete") and (file is None):
            check_url = url + progress_id
            check_response = r.request("GET", check_url, headers=headers)
            file = check_response.json()["result"]["file"]
            check_progress = check_response.json()["result"]["percentComplete"]
        download_url = url + progress_id + '/file'
        download_request = r.get(download_url, headers=headers, stream=True)
        return download_request

    # Version 3 Code
    def setup_request_v3(self, survey=None, payload=None):
        ''' This method sets up the request and handles the setup of the request for the survey.'''

        assert survey != None, 'Hey There! The survey parameter cannot be None. You need to pass in a survey ID ' \
                               'as a string into the survey parameter.'
        assert isinstance(survey, str) == True, 'Hey There! The survey parameter must be of type string.'
        assert len(
            survey) == 18, 'Hey there! It looks like your survey ID is a the incorrect length. It needs to ' \
                           'be 18 characters long. Please try again.'
        assert survey[
               :3] == 'SV_', 'Hey there! It looks like your survey ID is incorrect. You can find the survey ID ' \
                             'on the Qualtrics site under your account settings. Please try again.'

        headers, url = self.header_setup(content_type=True, xm=False, path='surveys/' + survey+ '/export-responses/')
        request = r.request("POST", url, data=json.dumps(payload), headers=headers)
        response = request.json()
        try:
            if response['meta']['httpStatus'] == '500 - Internal Server Error':
                raise Qualtrics500Error('500 - Internal Server Error')
            elif response['meta']['httpStatus'] == '503 - Temporary Internal Server Error':
                raise Qualtrics503Error('503 - Temporary Internal Server Error')
            elif response['meta']['httpStatus'] == '504 - Gateway Timeout':
                raise Qualtrics504Error('504 - Gateway Timeout')
            elif response['meta']['httpStatus'] == '400 - Bad Request':
                raise Qualtrics400Error(
                    'Qualtrics Error\n(Http Error: 400 - Bad Request): There was something invalid about the request.')
            elif response['meta']['httpStatus'] == '401 - Unauthorized':
                raise Qualtrics401Error(
                    'Qualtrics Error\n(Http Error: 401 - Unauthorized): The Qualtrics API user could not be '
                    'authenticated or does not have authorization to access the requested resource.')
            elif response['meta']['httpStatus'] == '403 - Forbidden':
                raise Qualtrics403Error(
                    'Qualtrics Error\n(Http Error: 403 - Forbidden): The Qualtrics API user was authenticated '
                    'and made a valid request, but is not authorized to access this requested resource.')
        except (Qualtrics500Error, Qualtrics503Error, Qualtrics504Error, Qualtrics400Error, Qualtrics401Error,
                Qualtrics403Error) as e:
            return logging.info(e)
        else:
            progress_id = response['result']['progressId']
            return progress_id, url, headers

    # Version 3 Code
    def send_request_v3(self, survey=None, payload=None):
        '''This method sends the request, and sets up the download request.'''
        is_file = None
        progress_id, url, headers = self.setup_request_v3(survey=survey, payload=payload)
        progress_status = "in progress"
        while progress_status != "complete" and progress_status != "failed" and is_file is None:
            check_url = url + progress_id
            check_request = r.request("GET", check_url, headers=headers)
            check_response = check_request.json()
            try:
                is_file = check_response["result"]["fileId"]
            except KeyError:
                pass
            progress_status = check_response["result"]["status"]
        try:
            if check_response['meta']['httpStatus'] == '500 - Internal Server Error':
                raise Qualtrics500Error('500 - Internal Server Error')
            elif check_response['meta']['httpStatus'] == '503 - Temporary Internal Server Error':
                raise Qualtrics503Error('503 - Temporary Internal Server Error')
            elif check_response['meta']['httpStatus'] == '504 - Gateway Timeout':
                raise Qualtrics504Error('504 - Gateway Timeout')
            elif check_response['meta']['httpStatus'] == '400 - Bad Request':
                raise Qualtrics400Error(
                    'Qualtrics Error\n(Http Error: 400 - Bad Request): There was something invalid about the request.')
            elif check_response['meta']['httpStatus'] == '401 - Unauthorized':
                raise Qualtrics401Error(
                    'Qualtrics Error\n(Http Error: 401 - Unauthorized): The Qualtrics API user could not be '
                    'authenticated or does not have authorization to access the requested resource.')
            elif check_response['meta']['httpStatus'] == '403 - Forbidden':
                raise Qualtrics403Error(
                    'Qualtrics Error\n(Http Error: 403 - Forbidden): The Qualtrics API user was authenticated '
                    'and made a valid request, but is not authorized to access this requested resource.')
        except (Qualtrics500Error, Qualtrics503Error, Qualtrics504Error, Qualtrics400Error, Qualtrics401Error,
                Qualtrics403Error) as e:
            return logging.info(e)
        else:
            download_url = url + is_file + '/file'
            download_request = r.get(download_url, headers=headers, stream=True)
            return download_request

    # Version 3 Code
    def get_survey_responses(self, survey=None, **kwargs):
        '''This function accepts the survey id, and returns the survey responses associated with that survey.
        :param useLabels: Instead of exporting the recode value for the answer choice, export the text of the answer
        choice. For more information on recode values, see Recode Values on the Qualtrics Support Page.
        :type useLabels: bool
        :param includeLabelColumns: For columns that have answer labels, export two columns: one that uses recode
        values and one that uses labels. The label column will has a IsLabelsColumn field in the 3rd header row.
        Note that this cannot be used with useLabels.
        :type includeLabelColumns: bool
        :param exportResponsesInProgress: Export only responses-in-progress.
        :type exportResponsesInProgress: bool
        :param limit: Maximum number of responses exported. This begins with the first survey responses recieved.
        So a Limit = 10, would be the surveys first 10 responses.
        :type limit: int
        :param seenUnansweredRecode: Recode seen-but-unanswered questions with this value.
        :type seenUnansweredRecode: int
        :param multiselectSeenUnansweredRecode: Recode seen-but-unanswered choices for multi-select questions
        with this value. If not set, this will be the seenUnansweredRecode value.
        :type multiselectSeenUnansweredRecode: int
        :param includeDisplayOrder: If true, include display order information in your export. This is useful
        for surveys with randomization.
        :type includeDisplayOrder: bool
        :param endDate: Only export responses recorded after the specified UTC date.
        Example Format: ('%Y-%m-%dT%H:%M:%SZ' => 2020-01-13T12:30:00Z)
        :type endDate: str
        :param startDate: Only export responses recorded after the specified UTC date.
        Example Format: ('%Y-%m-%dT%H:%M:%SZ'=> 2020-01-13T12:30:00Z)
        :type startDate: str
        :param timeZone: Timezone used to determine response date values. If this parameter is not provided,
        dates will be exported in UTC/GMT.
        See (https://api.qualtrics.com/instructions/docs/Instructions/dates-and-times.md) for the available timeZones.
        :type timeZone: str
        :param survey: This is the id associated with a given survey.
        :return: a Pandas DataFrame
        '''
        dynamic_payload = {"format": 'csv'}
        for key in list(kwargs.keys()):
            assert key in ['useLabels', 'includeLabelColumns', 'exportResponsesInProgress', 'limit',
                           'seenUnansweredRecode', 'multiselectSeenUnansweredRecode', 'includeDisplayOrder',
                           'startDate', 'endDate',
                           'timeZone'], "Hey there! You can only pass in parameters with names in the list, " \
                                        "['useLabels', 'includeLabelColumns', 'exportResponsesInProgress'" \
                                        ", 'limit', 'seenUnansweredRecode', 'multiselectSeenUnansweredRecode'" \
                                        ", 'includeDisplayOrder', 'startDate', 'endDate', 'timeZone']"
            if key == 'useLabels':
                assert 'includeLabelColumns' not in list(
                    kwargs.keys()), 'Hey there, you cannot pass both the "includeLabelColumns" and the ' \
                                    '"useLabels" parameters at the same time. Please pass just one and try again.'
                assert isinstance(kwargs['useLabels'],
                                  bool), 'Hey there, your "useLabels" parameter needs to be of type "bool"!'
                dynamic_payload.update({'useLabels': kwargs[(key)]})
            elif key == 'exportResponsesInProgress':
                assert isinstance(kwargs['exportResponsesInProgress'],
                                  bool), 'Hey there, your "exportResponsesInProgress" parameter ' \
                                         'needs to be of type "bool"!'
                dynamic_payload.update({'exportResponsesInProgress': kwargs[(key)]})
            elif key == 'limit':
                assert isinstance(kwargs['limit'], int), 'Hey there, your "limit" parameter needs to be of type "int"!'
                dynamic_payload.update({'limit': kwargs[(key)]})
            elif key == 'seenUnansweredRecode':
                assert isinstance(kwargs['seenUnansweredRecode'],
                                  int), 'Hey there, your "seenUnansweredRecode" parameter needs to be of type "int"!'
                dynamic_payload.update({'seenUnansweredRecode': kwargs[(key)]})
            elif key == 'multiselectSeenUnansweredRecode':
                assert isinstance(kwargs['multiselectSeenUnansweredRecode'],
                                  int), 'Hey there, your "multiselectSeenUnansweredRecode" parameter ' \
                                        'needs to be of type "int"!'
                dynamic_payload.update({'multiselectSeenUnansweredRecode': kwargs[(key)]})
            elif key == 'includeLabelColumns':
                assert isinstance(kwargs['includeLabelColumns'],
                                  bool), 'Hey there, your "includeLabelColumns" parameter needs to be of type "bool"!'
                assert 'useLabels' not in list(
                    kwargs.keys()), 'Hey there, you cannot pass both the "includeLabelColumns" and the "useLabels" ' \
                                    'parameters at the same time. Please pass just one and try again.'
                dynamic_payload.update({'includeLabelColumns': kwargs[(key)]})
            elif key == 'includeDisplayOrder':
                assert isinstance(kwargs['includeDisplayOrder'],
                                  bool), 'Hey there, your "includeDisplayOrder" parameter needs to be of type "bool"!'
                dynamic_payload.update({'includeDisplayOrder': kwargs[(key)]})
            elif key == 'startDate':
                assert isinstance(kwargs['startDate'],
                                  str), 'Hey there, your "startDate" parameter needs to be of type "str"!'
                start_date = parse(timestr=kwargs[(key)])
                dynamic_payload.update({'startDate': start_date.strftime('%Y-%m-%dT%H:%M:%SZ')})
            elif key == 'endDate':
                assert isinstance(kwargs['endDate'],
                                  str), 'Hey there, your "endDate" parameter needs to be of type "str"!'
                end_date = parse(timestr=kwargs[(key)])
                dynamic_payload.update({'endDate': end_date.strftime('%Y-%m-%dT%H:%M:%SZ')})
            elif key == 'timeZone':
                assert isinstance(kwargs['timeZone'],
                                  str), 'Hey there, your "timeZone" parameter needs to be of type "str"!'
                dynamic_payload.update({'timeZone': kwargs[(key)]})
        download_request = self.send_request_v3(survey=survey, payload=dynamic_payload)
        with zipfile.ZipFile(io.BytesIO(download_request.content)) as survey_zip:
            for s in survey_zip.infolist():
                df = pd.read_csv(survey_zip.open(s.filename))
                return df

    # Version 3 Code
    def get_survey_questions(self, survey=None):
        '''This method returns a DataFrame containing the survey questions and the Question IDs.

        :param survey: This is the id associated with a given survey.
        :return: a Pandas DataFrame
        '''
        df = self.get_survey_responses(survey=survey, limit=2)
        questions = pd.DataFrame(df[:1].T)
        questions.columns = ['Questions']
        return questions

    def get_survey_response(self, survey=None, response=None, verbose=False):
        ''' This method retrieves a single response from a given survey. '''

        assert survey != None, 'Hey There! The survey parameter cannot be None. You need to pass in a survey ID ' \
                               'as a string into the survey parameter.'
        assert response != None, 'Hey There! The response parameter cannot be None. You need to pass in a response ID ' \
                                 'as a string into the response parameter.'
        assert isinstance(survey, str) == True, 'Hey There! The survey parameter must be of type string.'
        assert isinstance(response, str) == True, 'Hey There! The response parameter must be of type string.'
        assert len(
            survey) == 18, 'Hey there! It looks like your survey ID is a the incorrect length. It needs to be 18 ' \
                           'characters long. Please try again.'
        assert len(
            response) == 17, 'Hey there! It looks like your response ID is a the incorrect length. It needs to be ' \
                             '17 characters long. Please try again.'
        assert survey[
               :3] == 'SV_', 'Hey there! It looks like your survey ID is incorrect. You can find the survey ID ' \
                             'on the Qualtrics site under your account settings. Please try again.'
        assert response[
               :2] == 'R_', 'Hey there! It looks like your response ID is incorrect. You can find the response ID ' \
                            'on the Qualtrics site under your account settings. Please try again.'

        headers, url = self.header_setup(content_type=True, xm=False, path='/surveys/' + survey + '/responses/'
                                                                           + response)
        request = r.request("GET", url, headers=headers)
        response = request.json()
        try:
            if response['meta']['httpStatus'] == '500 - Internal Server Error':
                raise Qualtrics500Error('500 - Internal Server Error')
            elif response['meta']['httpStatus'] == '503 - Temporary Internal Server Error':
                raise Qualtrics503Error('503 - Temporary Internal Server Error')
            elif response['meta']['httpStatus'] == '504 - Gateway Timeout':
                raise Qualtrics504Error('504 - Gateway Timeout')
            elif response['meta']['httpStatus'] == '400 - Bad Request':
                raise Qualtrics400Error(
                    'Qualtrics Error\n(Http Error: 400 - Bad Request): There was something invalid about the request.')
            elif response['meta']['httpStatus'] == '401 - Unauthorized':
                raise Qualtrics401Error(
                    'Qualtrics Error\n(Http Error: 401 - Unauthorized): The Qualtrics API user could not be '
                    'authenticated or does not have authorization to access the requested resource.')
            elif response['meta']['httpStatus'] == '403 - Forbidden':
                raise Qualtrics403Error(
                    'Qualtrics Error\n(Http Error: 403 - Forbidden): The Qualtrics API user was authenticated '
                    'and made a valid request, but is not authorized to access this requested resource.')
        except (Qualtrics503Error, Qualtrics504Error) as e:
            # Recursive call to handle Internal Server Errors
            return self.get_survey_response(self, survey=survey, response=response, verbose=verbose)
        except (Qualtrics500Error, Qualtrics400Error, Qualtrics401Error, Qualtrics403Error) as e:
            # Handle Authorization/Bad Request Errors
            return logging.info(e)
        else:
            if verbose == True:
                return response['meta']['httpStatus'], response['result']
            else:
                return response['result']
        return

    def get_survey_ids(self):
        surveys = {
            'SV_2sF0lL5xtQXIne6': 'CXP - Regional Chat & Social Evaluation Form',
            'SV_3HFLHRXEuuiAXrw': 'CXP - Regional Inbound Evaluation Form 2023',
            'SV_5upLkYzTfgaKVNA': 'CXP QA - Regional Email Evaluation Form 2023',
            'SV_9uawzeDcQ0iGIo6': 'CXP - Regional IR SWAT Evaluation Form 2023',
            'SV_38cVrzsm4PKXXMy': 'CXP - Regional IR Ops Evaluation Form 2023',
            'SV_0lj6LUAjeea9eHs': 'CXP - Regional Chat & Social Evaluation Form - Ibex',
            'SV_0D67NYebU0yRk8u': 'CXP - Regional Inbound Evaluation Form 2023 - Ibex',
            'SV_0IpnZ91lDuTT64e': 'CXP QA - Regional Email Evaluation Form 2023 - Ibex',
            'SV_bOT45tz9MESahz8': 'CXP - Regional Chat & Social Evaluation Form - Tribe',
            'SV_ezJLI9Y7IKbq7Yi': 'CXP - Regional Inbound Evaluation Form 2023 - Tribe',
            'SV_d7pbONN7vMn3Xo2': 'CSAT Feedback inside chatbot - PK',
            'SV_essQHJdk3NRjgfs': 'CSAT Feedback inside chatbot - BD',
            'SV_e9vFqEVePE0Ab6m': 'CSAT Feedback inside chatbot - NP',
            'SV_czEkIccnfB20FwO': 'CSAT Feedback inside chatbot - LK',
            'SV_5ph2YRidUZIvLgi': '[BD] Chatbot Customer Satisfaction Survey', #Imp
            'SV_bgtRgRk1jpMAKzA': '[LK] Chatbot Customer Satisfaction Survey', #Imp
            'SV_aVQW5QPSeqmMHY2': '[NP] Chatbot Customer Satisfaction Survey', #Imp
            'SV_0dYCk9HLrWClDRI': '[PK] Chatbot Customer Satisfaction Survey', #Imp
            'SV_4SHhEXTgmNxKlcq': 'API Testing',
            'SV_b7b6zC2TLNQwczY': 'Buyer NPS - BD', #Imp
            'SV_42Zy8WJbwBfDBNY': 'Buyer NPS - LK', #Imp
            'SV_50GE9CFMggwL6Qe': 'Buyer NPS - MM', #Imp
            'SV_dgm9A1ZWJMwu97w': 'Buyer NPS - NP', #Imp
            'SV_eKayeyTWg6zhZmm': 'Buyer NPS - PK', #Imp
            # 'SV_eX5GM3uRO4GKxcW': 'Buyer NPS - PK - Copy',
            # 'SV_77dypIU2AZK3KyG': 'Chatbot CSAT Survey Test',
            # 'SV_5jrj5CZYZoY9r0y': 'Cohort 1 | BD',
            # 'SV_e3QlL3ZAmEgoJEy': 'Cohort 1 | LK',
            # 'SV_0T9KOiD7R67cH6m': 'Cohort 1 | MM',
            # 'SV_dbw9yISa7YRyEd0': 'Cohort 1 | NP',
            # 'SV_eIMDXrcc5MvQGkS': 'Cohort 1 | PK',
            # 'SV_bdBv9He41snXW4e': 'Cohort 2 | BD',
            # 'SV_8kP3sIGHopHUK10': 'Cohort 2 | LK',
            # 'SV_1EKpt7HWyTkFD4q': 'Cohort 2 | MM',
            # 'SV_dg1YQT6aeIqoDMG': 'Cohort 2 | NP',
            # 'SV_9p40HZbWlNE6yCq': 'Cohort 2 | PK',
            # 'SV_dmY465G24X3h0x0': 'Cohort 3 | BD',
            # 'SV_6LMR3xVqSUZ9VP0': 'Cohort 3 | LK',
            # 'SV_d1e0dmGwMKqzwkC': 'Cohort 3 | MM',
            # 'SV_cSgi94x45yYoLau': 'Cohort 3 | NP',
            # 'SV_d6k4HAdI7aEsmMK': 'Cohort 3 | PK',
            # 'SV_23HXk4LoQ82SFxQ': 'Cohort 4 | BD',
            # 'SV_0CIPSs41LjSDBMa': 'Cohort 4 | LK',
            # 'SV_6P5rKYJpl69WysS': 'Cohort 4 | MM',
            # 'SV_7WIvrWz5A0iN9NI': 'Cohort 4 | NP',
            # 'SV_4JEjuHZU8P82mN0': 'Cohort 4 | PK',
            # 'SV_0CHhlCoCFEuGdue': 'Collection Point Study | Oct \'23',
            # 'SV_9sMi8uKPyRuFixo': 'How Was Your Experience Last 11.11?',
            # 'SV_09xxdnRB0d5GJEi': 'Pop Up Ads - Test Form',
            # 'SV_3DixsP2x7aAW4Ae': 'Returns - BD - Cohort 1',
            # 'SV_80tlo6HPEdPtnIq': 'Returns - BD - Cohort 2',
            # 'SV_0VrhSWsqzHKA0tw': 'Returns - LK - Cohort 1',
            # 'SV_1LayrvEDWCFZXmu': 'Returns - LK - Cohort 2',
            # 'SV_eqzGg7PWmzWKQ4K': 'Returns - MM - Cohort 1',
            # 'SV_8IKHQjnkf2ekup0': 'Returns - MM - Cohort 2',
            # 'SV_esV8HNiJ09oowUm': 'Returns - NP - Cohort 1',
            # 'SV_cwpw5yjLYe5km22': 'Returns - NP - Cohort 2',
            # 'SV_3agI2UOZCbKw8nk': 'Returns - PK - Cohort 1',
            # 'SV_d3ZpebyNgnikWKq': 'Returns - PK - Cohort 2',
            'SV_by24Dq6V5np8dgi': 'Seller NPS - LK', #Imp
            'SV_6JSMnlv0xETt36e': 'Seller NPS - PK', #Imp
            # 'SV_4Ox4Bgkun9NQBUy': 'Seller NPS PK (English)',
            'SV_3KpGafewZ0yc7WK': 'Seller PSAT - MM', #Imp
            'SV_07mS6eZpQAAeVM2': 'Seller PSAT - NP', #Imp
            'SV_2sJ3jvWNCQwy0bY': 'Seller PSAT- BD' #Imp
            # 'SV_51JGB5ccW9UPF1I': 'Untitled project',
            # 'SV_dimCgczpaInO2Ro': 'Untitled project'
        }

        return surveys


#surveys = {'SV_7WIvrWz5A0iN9NI': 'Cohort 4 | NP', 'SV_4SHhEXTgmNxKlcq': 'API Testing'}
pakistan_timezone = pytz.timezone('Asia/Karachi')
utc_timezone = pytz.timezone('UTC')

current_datetime_pakistan = datetime.now(pakistan_timezone)
# current_datetime_pakistan = datetime(2023, 9, 16, 3, 0, 0, tzinfo=pakistan_timezone)
endDate_pakistantz = current_datetime_pakistan.replace(hour=23, minute=59, second=59, microsecond=59) - timedelta(
    days=1)
startDate_pakistantz = endDate_pakistantz - timedelta(days=1)

startDate_utc = startDate_pakistantz.astimezone(utc_timezone)
endDate_utc = endDate_pakistantz.astimezone(utc_timezone)

logging.info('Date Range from ' + str(startDate_utc) + ' to ' + str(endDate_utc))

alldfs = list()
surveys = Responses().get_survey_ids()
logging.info('Total Surveys to run: ' + str(len(list(surveys.keys()))))


for survey_id in list(surveys.keys()):
    logging.info('Running Survey: ' + str(survey_id) + " : " + surveys[survey_id])

    #For Bulk Upload
    survey = Responses().get_survey_responses(survey=survey_id)

    #For Delta Upload
    #survey = Responses().get_survey_responses(survey=survey_id, startDate=str(startDate_utc), endDate=str(endDate_utc))

    logging.info('CSV Size: ' + str(len(survey)) + ' rows.')
    questionIds = survey.columns
    questionDesc = survey.iloc[0].tolist()
    questionImportIds = survey.iloc[1].tolist()

    dict_questionDesc = dict(zip(questionIds, questionDesc))
    dict_questionImportIds = dict(zip(questionIds, questionImportIds))

    idCol = ['StartDate', 'EndDate', 'Status', 'IPAddress', 'Progress', 'Duration (in seconds)', 'Finished',
             'RecordedDate', 'ResponseId', 'RecipientLastName', 'RecipientFirstName', 'RecipientEmail',
             'ExternalReference', 'LocationLatitude', 'LocationLongitude', 'DistributionChannel', 'UserLanguage']

    questions = list()
    for q in questionIds:
        if q not in idCol:
            questions.append(q)

    logging.info('Questions: ' + str(questions))

    filterDf = survey.iloc[3:]
    unpivotDf = pd.melt(filterDf, id_vars=idCol, value_vars=questions, var_name='questions', value_name='answers')
    unpivotDf['questionDesc'] = unpivotDf['questions'].map(dict_questionDesc)
    unpivotDf['questionImportId'] = unpivotDf['questions'].map(dict_questionImportIds)
    unpivotDf['survey_id'] = survey_id
    unpivotDf['survey_name'] = surveys[survey_id]

    logging.info('Dataset Transformation. Rows: ' + str(len(unpivotDf)))

    alldfs.append(unpivotDf)
    logging.info('Process completed for survey: ' + str(survey_id))

logging.info('All surveys done.')
finalDf = pd.concat(alldfs)

logging.info('Pushing to DCloud')
finalDf.to_csv(responses_filepath, index=False)
logging.info('End with Success. File saved at: ' + str(responses_filepath))
