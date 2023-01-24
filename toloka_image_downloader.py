import json
import os
import toloka.client as toloka
import pandas as pd
import requests
from tqdm import tqdm

df_src = pd.read_excel('data.xlsx', sheet_name='main')
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)

OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}

toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')
print(toloka_client.get_requester())

assigments_set = set()

list_of_pools_passport = []
list_of_pools_30 = []

full_df = pd.DataFrame(columns=['INPUT:id', 'OUTPUT:img', 'HINT:text', 'HINT:default_language',
       'ASSIGNMENT:link', 'ASSIGNMENT:assignment_id', 'ASSIGNMENT:worker_id',
       'ASSIGNMENT:status', 'ASSIGNMENT:started', 'ACCEPT:verdict',
       'ACCEPT:comment'], data=None)

for pool in list_of_pools_30:
    df = toloka_client.get_assignments_df(pool, status = ['APPROVED', 'SUBMITTED', 'REJECTED'])
    full_df = pd.concat([full_df, df])

full_df_passports = pd.read_csv('all_results.tsv', sep='\t')
full_df_30 = pd.read_csv('all_results_30.tsv', sep='\t')

for i in df_src['id_task']:
    i = str(i).split(' / ')
    for a in i:
        for b in a.split(' '):
            if len(b) > 10:
                assigments_set.add(b)

for assigment_link in tqdm(assigments_set):
    if 'https://toloka.yandex.ru/task/' in assigment_link:
        print(assigment_link)
        if 'https://toloka.yandex.ru/task/' in assigment_link:
            assignment_id = assigment_link.split('task/')[1].split('/')[1]
        print(assignment_id)
        worker_id_30 = full_df_30[full_df_30['ASSIGNMENT:assignment_id']==assignment_id]['ASSIGNMENT:worker_id'].values[0]
        worker = requests.get(url='https://toloka.yandex.ru/api/new/requester/workers/' + worker_id_30,
                            headers=HEADERS).json()
        age = worker['age']
        if 'gender' in worker:
            gender = worker['gender']
        else:
            gender = 'None'
        country_two_letters = worker['country']
        country_list = requests.get(url='https://yastatic.net/s3/toloka/p/requester/toloka.en.f8134e9b10b398400049.json').content
        country = json.loads(country_list)[f'country:{country_two_letters}']
        if 'cityId' in worker:
            city = requests.get(url='https://toloka.yandex.ru/api/ctx/geobase/regions/%s?lang=EN' % worker['cityId'],
                                headers=HEADERS).json()['name']
        else:
            city = 'None'
        dir_name = f'{worker_id_30}_{gender}_{age}_{city}_{country}'
        if not os.path.exists(f'dirs/{dir_name}'):
            os.makedirs(f'dirs/{dir_name}')
        output_images = full_df_30[full_df_30['ASSIGNMENT:assignment_id'] == assignment_id]['OUTPUT:img'].values[0]
        num = 1
        for image in output_images.split(','):
            name = str(num)
            if not os.path.exists(f'dirs/{dir_name}/{name}.jpg'):
                image_url = f'https://toloka.yandex.ru/api/v1/attachments/{image}/download'
                print(image)
                num += 1
                img_data = requests.get(image_url, headers=HEADERS, timeout=600)
                with open(f'dirs/{dir_name}/{name}.jpg', 'wb') as handle:
                    handle.write(img_data.content)
                    handle.close()
        output_image_passport = full_df_passports[full_df_passports['ASSIGNMENT:worker_id']==worker_id_30]['OUTPUT:img'].values[0]
        image_url_passport = f'https://toloka.yandex.ru/api/v1/attachments/{output_image_passport}/download'
        print(output_image_passport)
        name = 'passport_photo'
        img_data = requests.get(image_url_passport, headers=HEADERS)
        if not os.path.exists(f'dirs/{dir_name}/{name}.jpg'):
            with open(f'dirs/{dir_name}/{name}.jpg', 'wb') as handle:
                handle.write(img_data.content)
                handle.close()
