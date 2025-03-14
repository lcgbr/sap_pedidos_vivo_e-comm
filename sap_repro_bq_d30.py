# Databricks notebook source
# MAGIC %md
# MAGIC ### Init Library

# COMMAND ----------

# dbutils.library.installPyPI("pandas", version="1.5.3")
# dbutils.library.restartPython() # Removes Python state, but some libraries might not work without calling this command.

# COMMAND ----------

import asyncio
import math
import requests
import urllib3
import time
import hashlib
import pandas as pd
from datetime import date, timedelta, datetime
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from apiclient.errors import HttpError
from pytz import timezone
from datetime import date, datetime, timedelta, timezone
# import datetime
from pytz import timezone
import uuid
import re
# import libify
# funcs = libify.importer(globals(), '/COE_PRD/GlobalFunctions/GlobalFunctions')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import warnings
warnings.filterwarnings("ignore")

from google.oauth2 import service_account
import timeit
import traceback
import sys
import httpx

KEY = 'credentials/key.json'
CREDENTIALS = service_account.Credentials.from_service_account_file(KEY)
MODE = 'async'
HTTPX_TIMEOUT = 300.0
MAX_RETRIES = 30
MAX_ASYNC_TASKS = 20

# COMMAND ----------

# MAGIC %md
# MAGIC #Function subtract day for reprocess

# COMMAND ----------

def subtract_days(qty_days):
  # Current time in UTC
  now_utc = datetime.now(timezone('UTC'))
  
  # Convert to America/Sao_Paulo time zone
#   now_local_time = now_utc.astimezone(timezone('America/Sao_Paulo'))
#   now_local_time = now_utc.astimezone(timezone('Europe/Istanbul'))
  
#   subtracted_date = now_local_time - timedelta(days=qty_days)
  subtracted_date = datetime.now() - timedelta(days=qty_days)
  return subtracted_date.strftime("%Y-%m-%d")

# dbutils.widgets.text(
#   name='end_date',
#   defaultValue=subtract_days(1),
#   label='End date'
# )

# dbutils.widgets.text(
#   name='start_date',
#   defaultValue=subtract_days(7),
#   label='Start date'
# )

# COMMAND ----------

def encrypt_string(hash_string):
  salt = ',s)S.X-p;SdwsA2&lR.dIy|SCg}bZ1{&7y^%kpk2u9V+{mEO%n3HccBYJIKhFujb'
  hash_string = str(hash_string)+salt
  sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
  return str(sha_signature)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Date Started and Ended

# COMMAND ----------

 # if (dbutils.widgets.get('start_date')):
 #   start_date = dbutils.widgets.get('start_date')
 # else:
 #   start_date = subtract_days(7) 
  
 # if (dbutils.widgets.get('end_date')):
 #   end_date = dbutils.widgets.get('end_date')
 # else:
 #   end_date = subtract_days(1)

# start_date = subtract_days(7)
# end_date = subtract_days(1)


# COMMAND ----------

# MAGIC %md
# MAGIC #Settings Variavel Global

# COMMAND ----------

##developer
# endpoint = 'api.c6o4pyfwdq-telefonic3-s1-public.model-t.cc.commerce.ondemand.com'
##production
endpoint = 'api.store.vivo.com.br'
filter_date_start = "?$filter=creationtime ge datetime"
filter_date_end = "creationtime le datetime"
maxItens = 1000
##variavel temp
orders_caracter = '%7Cnull'

# now_utc = datetime.now(timezone('UTC'))
# Convert to America/Sao_Paulo time zone
# hour = now_utc.astimezone(timezone('America/Sao_Paulo')).strftime('%H')
# minutes = now_utc.astimezone(timezone('America/Sao_Paulo')).strftime('%M')
hour = datetime.now().strftime('%H')
minutes = datetime.now().strftime('%M')

hour = '0'+str(hour) if int(hour) <= 9 else hour

# COMMAND ----------

# MAGIC %md
# MAGIC #Function Auth

# COMMAND ----------

def auth():
  url = f"https://{endpoint}/authorizationserver/oauth/token?client_id=bigquery&client_secret=894725-AmsSOkp%23!&grant_type=client_credentials"

  headers = {
    'Accept': 'application/json',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }

  if MODE == 'async':
    response = httpx.post(url, headers={"User-Agent": "insomnia/8.4.5", 'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88'})
  else:
    response = requests.request("POST", url)

  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)

  return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC # Disponibility service

# COMMAND ----------

token = auth()
# count_error = 0

# while count_error <= 5:
#   try:
#     token = auth()
    
#     if token['error']:
#         count_error += 1
#     else:    
#       break
#   except:
#     count_error += 1

# COMMAND ----------

# MAGIC %md
# MAGIC #Function normalize DataFrame

# COMMAND ----------

def normalizeDictOrders(frame):
  columns_rename = {
              'globalDiscountValuesInternal':'ValorTotalDescontoInterno',
              'guid':'IdUnicoTransacao',
              'name':'Nome',
              'totalDiscounts':'DescontoTotal',
              'code':'orderid',
              'VACreationTime':'DataCriacao',
              'totalPrice':'PrecoTotal',
              'integrationKey':'ChaveIntegracao',
              'VAAddressCreationTime':'DataCriacaoEndereco',
              'isocode':'CodigoIso',
              'language':'idioma',
              'invoiceSeriesNumber':'NumeroSeriePagamento',
              'VAConsignmentCreationTime':'DataCriacaoRemessa',
              'trackingID':'CodigoRastreio',
              'shippingDate':'DataEntrega',
              'invoiceNumber':'NumeroPagamento',
              'requestId':'CodigoRequisicao',
              'VAPaymentTransactionTime':'DataCriacaoPagamento',
              'requestToken':'TokenRequisicao',
              'paymentProvider':'ProvedorPagamento',
              'transactionStatus':'StatusTransacao',
              'transactionStatusDetails':'DescricaoStatusTransacao',
              'amount':'valor',
              'quantity':'Quantidade',
              'giveAway':'Doar',
              'discountValuesInternal':'ValorDescontoInterno',
              'basePrice':'PrecoBase',
              'commercialName':'NomeComercial',
              'ean':'Ean',
              'manufacturerAID':'CodigoFabricante',
              'manufacturerName':'NomeFabricante',
              'version':'VersaoOrigem',
              'id':'Codigo',
              'VACPF':'Cpf',
              'VAInstallment':'QuantidadeParcelamento',
              'VAInstallmentValue':'ValorParcelamento',
              'VACardType':'BandeiraCartao',
              'VAMobile':'Celular',
              'phone':'Telefone',
              'productCode':'Sku',
              'overSelling':'EstoqueVirtual',
              'VAStockCreationTime':'DataCriacaoEstoque',
              'available':'EstoqueFisico',
              'reserved':'Reservado',
              'VAStockDeliveryTime':'DataPrevistadeEstoque',
              'VAEntryDiscount':'DescontoCarrinho'
            }

  for column in frame.columns:
    if column in columns_rename:
      frame.rename(columns={column:columns_rename[column]}, inplace=True)
  return frame

# COMMAND ----------

# MAGIC %md
# MAGIC # BigQuery

# COMMAND ----------

def injestBigQuery(table, data):
    
    bigquery_client = bigquery.Client(credentials=CREDENTIALS)
    dataset_ref = bigquery_client.dataset('coe_datalake')
    dataset_table = dataset_ref.table(table)
    
    settings_load = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    # print('injestBigQuery DESABILITADO')
    job = bigquery_client.load_table_from_dataframe(data, dataset_table, job_config=settings_load)

    job.result()

# COMMAND ----------

# def deleteBigQuery(dt_start):
def deleteBigQuery(min_date, max_date):  
  
#   dt_start = dt_start.replace('T', ' ')
#   dt_end = dt_end.replace('T', ' ')
  
  bigquery_client = bigquery.Client(credentials=CREDENTIALS)
  
  # stmt = f"""
  #          delete FROM `telefonica-digitalsales.coe_datalake.sap_commerce_pedidos` where date(data_request) = '{dt_start}';
  #        """

  stmt = f"""
           delete FROM `telefonica-digitalsales.coe_datalake.sap_commerce_pedidos` WHERE DATE(data_criacao_pedido) BETWEEN '{min_date}' AND '{max_date}';
         """         

  
  # print('deleteBigQuery DESABILITADO')
  query_job = bigquery_client.query(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC #Endpoints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quantity Stock

# COMMAND ----------

def getQuantityStock():

  url = f"https://{endpoint}/odata2webservices/OutboundStock/StockLevels/$count"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }

  if MODE == 'async':
    response = httpx.get(url, headers=headers, verify=False)
  else:
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)

  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  response = response.json()

  return response

# COMMAND ----------

# getQuantityStock()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quantity Orders

# COMMAND ----------

def getQuantityOrder(date_start, date_end):

  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders/$count{filter_date_start}'{date_start}' and {filter_date_end}'{date_end}'"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }

  if MODE == 'async':
    response = httpx.get(url, headers=headers, verify=False)
  else:
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)

  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  response = response.json()

  return response

# COMMAND ----------

# getQuantityOrder('2022-06-15T00:00:00', '2022-06-15T23:59:59')

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Orders

# COMMAND ----------

def getOrders(date_start, date_end):
  
  qtdOrder = getQuantityOrder(date_start, date_end)
  frame_orders = pd.DataFrame()
  
  # totalPages = round(qtdOrder/maxItens)
  # page = 0
  totalPages = math.ceil(qtdOrder/maxItens)
  page = 1
  print(qtdOrder)
  print(totalPages)
  while page <= totalPages:
    mx = page-1 if page == 1 else ((maxItens) * page)-1000
    # url = f"https://{endpoint}/odata2webservices/OutboundOrder/Orders{filter_date_start}'{date_start}' and {filter_date_end}'{date_end}'&$top={maxItens}&$skiptoken={page}"
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders{filter_date_start}'{date_start}' and {filter_date_end}'{date_end}'&$top={maxItens}&$skiptoken={mx}"
    
    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      try:
        if MODE == 'async':
          #   response = httpx.get(url, headers=headers, verify=False)

          # transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
          # with httpx.AsyncClient(verify=False, transport=transport) as client:
          #   response = client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
        
          with httpx.Client(verify=False) as client:
            response = client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
        else:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
      except:
        print('Connection Refused - List Orders')
        print('Let me Sleep for 3 seconds')
        time.sleep(3)
      
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
    response = response.json()
    
    try:
      response = pd.DataFrame.from_dict(response['d']['results'])
      response['globalDiscountValuesInternal'] = response['globalDiscountValuesInternal'].map(lambda x: x.split('#')[1] if len(x) > 2 else 0.0)
      
      #response.drop(['__metadata','creationtime','date','user','paymentInfo','discounts',
      #                    'deliveryAddress','consignments','entries','status','paymentTransactions',
      #                    'appliedCouponCodes','integrationKey','deliveryInfo','vendor'], axis=1, inplace=True)

      response.drop(['__metadata','creationtime','date','user','paymentInfo','discounts',
                          'deliveryAddress','consignments','entries','status','paymentTransactions',
                          'appliedCouponCodes','integrationKey','deliveryInfo','vendor',
                          'modifiedtime', 'orderProcess', 'historyEntries', 'vivoOrderType',
                          'fraudReports', 'deliveryStatus', 'salesApplication'], axis=1, inplace=True)
      
      response['page'] = page
      page +=1
    except:
      page +=1
    

    frame_orders = frame_orders.append(response)
  
  frame_orders['numero_pedido_shopcode'] = frame_orders['code'].astype(str) + '|' + frame_orders['shopCode'].map(lambda x: 'null' if str(x) == 'None' else x)
  
  frame_orders.rename(columns={
    'guid':'id_sistema',
    'name':'nome_ordem',
    'email':'email_ordem',
    'totalDiscounts':'total_desconto',
    'code':'numero_pedido',
    'VACreationTime':'data_criacao_pedido',
    'totalPrice':'valor_total_pedido',
    'phone':'telefone',
    'integrationKey':'chave_integracao',
    'globalDiscountValuesInternal':'valor_disconto_interno_carrinho',
    'shopCode':'shop_code',
    'asbId': 'asb_id'
  }, inplace=True)
  
  
#   frame_orders = normalizeDictOrders(frame_orders)

  
  return frame_orders

# COMMAND ----------

async def getVendor(orderid):
    try:
        url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/vendor"

        payload={}
        headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token["access_token"]}',
        "User-Agent": "insomnia/8.4.5",
        'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
        }

        response = ''
        while response == '':
            if MODE == 'async':
              transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
              async with httpx.AsyncClient(verify=False, transport=transport) as client:
                response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
            else:
              try:
                  response = requests.request("GET", url, headers=headers, data=payload, verify=False)
              except:
                  print('Connection Refused - Delivery Days')
                  print('Let me Sleep for 3 seconds')
                  time.sleep(3)
        
        if response.status_code == 403:
          print(response.status_code)
          print(url)
          print(response.text)
        response = response.json()

        del response['d']['__metadata']

        response = pd.DataFrame([response['d']])
        response['numero_pedido'] = orderid.split('|')[0]
        response.drop(['integrationKey'],axis=1, inplace=True)
        response.rename(columns={'uid':'login'}, inplace=True)
        
    except:
        response = pd.DataFrame()
        response = response.append({'numero_pedido':orderid, 'login':0}, ignore_index=True)


    return response

# COMMAND ----------

async def getOrder_deliverayDay(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/deliveryInfo"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - Delivery Days')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
    response = response.json()

    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])
    response['numero_pedido'] = orderid.split('|')[0]

    response.rename(columns={'deliveryTimeInDays':'dias_entrega'}, inplace=True)
  except:
    response = pd.DataFrame()
    response = response.append({'dias_entrega':0, 'numero_pedido':orderid.split('|')[0]}, ignore_index=True)
  

  return response[['numero_pedido','dias_entrega']]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Orders

# COMMAND ----------

def getOrder(orderid):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  response = response.json()

  del response['d']['__metadata']

  response = pd.DataFrame([response['d']])
  
  response['globalDiscountValuesInternal'] = response['globalDiscountValuesInternal'].map(lambda x: x.split('#')[1] if len(x) > 2 else 0.0)

  # #drop column
  response.drop(['creationtime','date','user','paymentInfo','discounts','deliveryAddress','consignments','entries','status','paymentTransactions','appliedCouponCodes','deliveryInfo'], axis=1, inplace=True)

  response.rename(columns={
    'guid':'id_sistema',
    'name':'nome_ordem',
    'email':'email_ordem',
    'totalDiscounts':'total_disconto',
    'code':'numero_pedido',
    'VACreationTime':'data_criacao_pedido',
    'totalPrice':'valor_total',
    'phone':'celular',
    'integrationKey':'chave_integracao_ordem',
    'globalDiscountValueInternal':'valor_disconto_interno_carrinho',
  }, inplace=True)
  
  return response


# COMMAND ----------

# getOrder('01835370')

# COMMAND ----------

# MAGIC %md
# MAGIC ## List All Cupons

# COMMAND ----------

async def getSalesApplication(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/salesApplication"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:  
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - salesApplication')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)
    
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
      
    response = response.json()

    # response = pd.DataFrame.from_dict(response['d'])

    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    response.drop(['integrationKey'],axis=1, inplace=True)
      
    response.rename(columns={
        'code':'sales_application'
      }, inplace=True)
    
  except:
    response = pd.DataFrame()
    response = response.append({
      'sales_application':'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  # if response.empty:
  #   exit
  # else:
  #   #drop column
  #   response.drop(['__metadata'], axis=1, inplace=True)

  return response

async def getDeliveryStatus(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/deliveryStatus"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - deliveryStatus')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)

    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)

    response = response.json()

    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    response.drop(['integrationKey'],axis=1, inplace=True)
      
    response.rename(columns={
        'code':'delivery_status'
      }, inplace=True)
    
  except:
    response = pd.DataFrame()
    response = response.append({
      'delivery_status':'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response


# COMMAND -------------------

async def getVivoOrderType(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/vivoOrderType"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - vivoOrderType')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)

    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)

    response = response.json()

    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    response.drop(['integrationKey'],axis=1, inplace=True)
      
    response.rename(columns={
        'code':'vivo_order_type'
      }, inplace=True)
    
  except:
    response = pd.DataFrame()
    response = response.append({
      'vivo_order_type':'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response

# COMMAND--------------------

def getOrdersAll_coupons():
  url = f"https://{endpoint}/odata2webservices/OutboundPromotion/PromotionSourceRules?$top=999999"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }
  
  response = ''
  while response == '':
    try:
      response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    except:
      print('Connection Refused - Cupons')
      print('Let me Sleep for 3 seconds')
      time.sleep(3)
      
  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])

  ##validate coupons
  response['coupons_flag'] = response['conditions'].map(lambda x: 'S' if 'coupons' in str(x) else 'N')

  ##value coupons
  response['coupons'] = response['conditions'].map(lambda x: re.findall('"value\":\["\w+',str(x)))
  response['coupons'] = response['coupons'].astype(str)
  response['coupons'] = response['coupons'].str.extract('(\w+\d+|\w+_\w+)')

  ##value discount rules
  response['coupons_rules'] = response['conditions'].map(lambda x: re.findall('\w+::vivoProductCatalog',str(x)))
  response['coupons_rules'] = response['coupons_rules'].astype(str)
  response['coupons_rules'] = response['coupons_rules'].str.extract('(\w+::vivoProductCatalog)')

  ##value skus
  response['skus_acessorios'] = response['coupons_rules'].map(lambda x: str(x).split(':')[0])

  ###value discount
  response['valor_desconto'] = response['actions'].map(lambda x: re.findall('"value":[0-9]+|"value":{"BRL":[0-9]+', str(x)))
  response['valor_desconto'] = response['valor_desconto'].astype(str)
  response['valor_desconto'] = response['valor_desconto'].str.extract('([0-9]+)')

  response['tipo'] = response['actions'].map(lambda x: 'Percentil' if len(re.findall('"value":[0-9]+', str(x))) > 0 else 'Real' if len(re.findall('value":{"BRL":[0-9]+', str(x))) > 0 else 'na')

  response.drop(['__metadata','localizedAttributes','actions','conditions'], axis=1, inplace=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coupon

# COMMAND ----------

async def getOrdersDetails_coupon(orderid):
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/appliedCouponCodes"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }
  
  response = ''
  while response == '':
    if MODE == 'async':
      transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
      async with httpx.AsyncClient(verify=False, transport=transport) as client:
        response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
    else:
      try:
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
      except:
        print('Connection Refused - Sku DataSheet')
        print('Let me Sleep for 3 seconds')
        time.sleep(3)
          
  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  response = response.json()
  
  dataframe = pd.DataFrame(columns=['cupom_interno','cupom_carrinho'])
  
  try:
    response = response['d']['results']

    cupom_string = []

    for i in range(0,len(response)):
      cupom_string.append(response[i]['value'])

    response = '|'.join(cupom_string) 

    try:
      coupom_interno = response.split('|')[0]
    except:
      coupom_interno = 'na'

    try:
      coupon_carrinho = response.split('|')[1]
    except:
      coupon_carrinho = 'na'
    
    dataframe = dataframe.append({'cupom_interno':coupom_interno,'cupom_carrinho':coupon_carrinho,'numero_pedido':orderid.split('|')[0]}, ignore_index=True)

    response = dataframe
  except:
    coupom_interno = 'na'
    coupon_carrinho = 'na'
    dataframe = dataframe.append({'cupom_interno':coupom_interno,'cupom_carrinho':coupon_carrinho,'numero_pedido':orderid.split('|')[0]}, ignore_index=True)
    response = dataframe
  
  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Users

# COMMAND ----------

async def getOrdersDetails_user(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/user"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }
    
    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - Users')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)
        
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
    response = response.json()
    
    del response['d']['__metadata']
    del response['d']['defaultPaymentAddress']
    del response['d']['defaultShipmentAddress']
    
    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    
    response.rename(columns={
      'uid':'codigo_unico_cliente',
      'name':'nome_cliente',
      'VACPF':'cpf',
      'VAMobile':'celular',
      'IntegrationKey':'chave_integracao_usuario'
    }, inplace=True)
  except:
    response = pd.DataFrame()
    response = response.append({
      'codigo_unico_cliente':'na',
      'nome_cliente':'na',
      'cpf':'na',
      'celular':'na',
      'chave_integracao_usuario':'',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)
  
  return response

# COMMAND ----------

# getOrdersDetails_user('00415018')

# COMMAND ----------

# MAGIC %md
# MAGIC ## PaymentInfo

# COMMAND ----------

async def getOrdersDetails_paymentInfo(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/paymentInfo"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }
    
    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - Payment Info')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)
        
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
    response = response.json()
    
    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    
    response.rename(columns={
      'VAInstallment':'qtd_parcelas',
      'VAInstallmentValue':'valor_parcela',
      'code':'codigo_ident_pgto',
      'VACardType':'bandeira_cartao',
      'integrationKey':'chave_integracao_pgto_info'
    }, inplace=True)
  except:
    response = pd.DataFrame()
    response = response.append({
      'qtd_parcelas':'na',
      'valor_parcela':'na',
      'codigo_ident_pgto':'na',
      'bandeira_cartao':'na',
      'chave_integracao_pgto_info':'',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discounts

# COMMAND ----------

def getOrdersDetails_discounts(orderid):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/discounts"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = ''
  while response == '':
    try:
      response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    except:
      print('Connection Refused - Discounts')
      print('Let me Sleep for 3 seconds')
      time.sleep(3)
      
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])

  if response.empty:
    exit
  else:
    #drop column
    response.drop(['__metadata'], axis=1, inplace=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## DeliveryAddress

# COMMAND ----------

async def getOrdersDetails_deliveryAddress(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/deliveryAddress"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - deliveryAddress')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)

    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)

    response = response.json()

    del response['d']['__metadata']
    del response['d']['region']

    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    response.drop(['creationtime', 'postalcode', 'streetnumber', 'town', 'complement', 
                   'VAAddressCreationTime', 'streetname'],axis=1, inplace=True)
      
    response.rename(columns={
        'id': 'region_id',
        'integrationKey': 'region_key'
      }, inplace=True)
    
  except:
    response = pd.DataFrame()
    response = response.append({
      'region_id': 'na',
      'region_key': 'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### Region

# COMMAND ----------

async def getOrdersDetails_deliveryAddressRegion(orderid, idDelivery):

  import urllib.parse
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Addresses('{urllib.parse.quote(idDelivery)}')/region"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - deliveryAddressRegion')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)

    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)

    response = response.json()

    del response['d']['__metadata']
    del response['d']['country']
    del response['d']['localizedAttributes']

    response = pd.DataFrame([response['d']])  
    response['numero_pedido'] = orderid.split('|')[0]
    response.drop(['name', 'isocode', 'integrationKey'],axis=1, inplace=True)
      
    response.rename(columns={
        'isocodeShort': 'regional'
      }, inplace=True)
    
  except:
    response = pd.DataFrame()
    response = response.append({
      'regional': 'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC #### Country

# COMMAND ----------

#ajust format send request regions (%7C -> encodeURI)

def getOrdersDetails_deliveryAddressCountry(keyIntegration):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Regions('{keyIntegration}')/country"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload, verify=False)
  response = response.json()

  frame = pd.DataFrame.from_dict(response['d'])

  if frame.empty:
    exit
  else:
  #   #drop columns dictionary
    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])

    response = normalizeDictOrders(response)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC #### localizedAttributes

# COMMAND ----------

#ajust format send request regions (%7C -> encodeURI)

def getOrdersDetails_deliveryAddressLocalizedAttributes(keyIntegration):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Regions('{keyIntegration}')/localizedAttributes"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload, verify=False)
  response = response.json()

  frame = pd.DataFrame.from_dict(response['d']['results'])

  if frame.empty:
    exit
  else:
    response = pd.DataFrame.from_dict(response['d']['results'])
    #drop columns dataframe
    response.drop(['__metadata'],axis=1, inplace=True)

    response = normalizeDictOrders(response)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consignments

# COMMAND ----------

async def getOrdersDetails_consignments(orderid):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/consignments"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }
    
    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - Consigments')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)
        
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
    response = response.json()

    dataframe = pd.DataFrame()

    frame = pd.DataFrame.from_dict(response['d']['results'])

    if frame.empty:
      dataframe = dataframe.append({
        'codigo_sap': 'na',
        'numero_serie_nf':'na',
        'data_criacao_nf':'na',
        'ov':'na',
        'codigo_rastreio':'na',
        'data_envio':'na',
        'numero_nf':'na',
        'chave_integracao_pgto_sap':'na',
        'sap_msg':'na',
        'numero_pedido':orderid.split('|')[0]
      }, ignore_index=True)
      response = dataframe
    else:
      response = pd.DataFrame.from_dict(response['d']['results'])
      #drop columns dataframe
      response.drop(['__metadata','creationtime','status'],axis=1, inplace=True)

      response.rename(columns={
        'sapCode':'codigo_sap',
        'invoiceSeriesNumber':'numero_serie_nf',
        'VAConsignmentCreationTime':'data_criacao_nf',
        'code':'codigo_rastreio',
        'trackingID':'ov',
        'shippingDate':'data_envio',
        'invoiceNumber':'numero_nf',
        'sapErrorMessage':'sap_msg',
        'integrationKey':'chave_integracao_pgto_sap'
      }, inplace=True)
      
      response['sap_msg'] = response.apply(lambda x: x.sap_msg if x.ov is None else 'na', axis=1)
      
      response = response.fillna('na')

      for column in response.columns:
        if not column in ['data_criacao_nf','data_envio']:
          string = []
          for i in response[column]:
                string.append(i)

          response[column] = '|'.join(string)

      response = response.drop_duplicates()
      response['numero_pedido'] = orderid.split('|')[0]
  except:
    response = pd.DataFrame()
    response = response.append({
      'codigo_sap':'na',
      'numero_serie_nf':'na',
      'data_criacao_nf':'na',
      'ov':'na',
      'codigo_rastreio':'na',
      'data_envio':'na',
      'numero_nf':'na',
      'chave_integracao_pgto_sap':'na',
      'sap_msg':'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response

# COMMAND ----------

# getOrdersDetails_consigments('00907001')

# COMMAND ----------

async def getOrdersDetails_consignmentsStatus(orderid, codigoRastreio):

  import urllib.parse

  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Consignments('{urllib.parse.quote(codigoRastreio)}')/status"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - consignmentsStatus')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)

    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)

    response = response.json()

    del response['d']['__metadata']

    response = pd.DataFrame([response['d']])
    response['numero_pedido'] = orderid.split('|')[0]
    #drop columns dataframe
    response.drop(['integrationKey'],axis=1, inplace=True)
    response.rename(columns={
      'code':'status_macro'
      }, inplace=True)
  
  except:
    response = pd.DataFrame()
    response = response.append({
      'status_macro': 'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)

  return response

# COMMAND ----------

# getOrdersDetails_consigmentsStatus('a00415017')

# COMMAND ----------

# MAGIC %md
# MAGIC ## PaymentTransactions

# COMMAND ----------

async def getOrdersDetails_paymentTransactions(orderid):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/paymentTransactions"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }
  
  response = ''
  while response == '':
    if MODE == 'async':
      transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
      async with httpx.AsyncClient(verify=False, transport=transport) as client:
        response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
    else:
      try:
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
      except:
        print('Connection Refused - Payment Transaction')
        print('Let me Sleep for 3 seconds')
        time.sleep(3)
      
  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)

  #if response.status_code == 400:
  #  print(response.status_code)
  #  print(url)
  #  print(response.text)

  response = response.json()  
  #if not response or 'd' not in response:
  #  raise ValueError(f"Invalid response for order {orderid}: {response}")
  #response = response.json()
  
  ############
  #if response.status_code == 400:
  #  response_json = response.json()
  #  error_message = response_json.get('error', {}).get('message', {}).get('value', 'Unknown Error')
  
      # send_slack_notification(orderid, error_message)
      # send_email_notification(orderid, error_message)

    # Retorna DataFrame padrão
  #  return pd.DataFrame([{
  #    'fii_status': 'na',
  #    'codigo_requisicao': 'na',
  #    'data_transacao_pgto': 'na',
  #    'codigo_transacao': 'na',
  #   'numero_pedido': orderid.split('|')[0],
  #    'fii_codigo': '',
  #    'fii_autorizacao': 'na',
  #    'token_requisicao': 'na',
  #    'provedor_pagamento': 'na',
  #    'chave_integracao_pgto_transacao': 'na'
  #  }])
  ###########

  try:
    frame = pd.DataFrame.from_dict(response['d']['results'])

    if frame.empty:
      response = response.append({
      'fii_status':'na',
      'codigo_requisicao':'na',
      'data_transacao_pgto':'na',
      'codigo_transacao':'na',
      'numero_pedido':orderid.split('|')[0],
      'fii_codigo':'',
      'fii_autorizacao':'na',
      'token_requisicao':'na',
      'provedor_pagamento':'na',
      'chave_integracao_pgto_transacao':'na'
      }, ignore_index=True)
      exit
    else:
      response = pd.DataFrame.from_dict(response['d']['results'])
      #drop columns dataframe
      response.drop(['__metadata','creationtime','entries'],axis=1, inplace=True)
      response = response[response['VAPaymentTransactionTime'] == max(response['VAPaymentTransactionTime'])]
      response['numero_pedido'] = orderid.split('|')[0]
      
      response.rename(columns={
        'VANormalizedFIStatus':'fii_status',
        'requestId':'codigo_requisicao',
        'VAPaymentTransactionTime':'data_transacao_pgto',
        'code':'codigo_transacao',
        'VANormalizedFICode':'fii_codigo',
        'VAAuthorizationCode':'fii_autorizacao',
        'requestToken':'token_requisicao',
        'paymentProvider':'provedor_pagamento',
        'integrationKey':'chave_integracao_pgto_transacao'
      }, inplace=True)
      
      response = response.reset_index()
      
  except:
    response = pd.DataFrame()
    response = response.append({
      'fii_status':'na',
      'codigo_requisicao':'na',
      'data_transacao_pgto':'na',
      'codigo_transacao':'na',
      'numero_pedido':orderid.split('|')[0],
      'fii_codigo':'',
      'fii_autorizacao':'na',
      'token_requisicao':'na',
      'provedor_pagamento':'na',
      'chave_integracao_pgto_transacao':'na'
    }, ignore_index=True)

  return response

# COMMAND ----------

# getOrdersDetails_paymentTransactions('00415018')

# COMMAND ----------

# MAGIC %md
# MAGIC ### PaymentTransaction Entries

# COMMAND ----------

async def getOrdersDetails_paymentTransactionsEntries(orderid,keyIntegration):
  
  import urllib.parse
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/PaymentTransactions('{urllib.parse.quote(keyIntegration)}')/entries"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }

  response = ''
  while response == '':
    if MODE == 'async':
      transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
      async with httpx.AsyncClient(verify=False, transport=transport) as client:
        response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
    else:
      try:
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
      except:
        print('Connection Refused - Payment Transaction Entries')
        print('Let me Sleep for 3 seconds')
        time.sleep(3)
      
  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  #if response.status_code == 400:
  #  print(response.status_code)
  #  print(url)
  #  print(response.text)
  response = response.json()

  try:
    frame = pd.DataFrame(response['d']['results'])

    if frame.empty:
      exit
    else:
      response = pd.DataFrame(response['d']['results'])
      #drop columns dataframe
      response.drop(['__metadata','creationtime'],axis=1, inplace=True)

      response = response[response['VAPTTime'] == max(response['VAPTTime'])]
      response['numero_pedido'] = orderid.split('|')[0]

      response.rename(columns={
        'transactionStatus':'status_transacao',
        'requestToken':'token_requisicao_pgto',
        'amount':'valor_transacao',
        'code':'codigo_transacao_pgto',
        'VAPTTime':'data_transacao_final',
        'transactionStatusDetails':'status_transacao_detalhada',
        'requestId':'codigo_requisicao_transacao',
        'integrationKey':'chave_integracao_pgto_final'
      }, inplace=True)
  except:
    response = pd.DataFrame()
    response = response.append({
        'status_transacao':'na',
        'token_requisicao_pgto':'na',
        'valor_transacao':'na',
        'codigo_transacao_pgto':'na',
        'data_transacao_final':'na',
        'status_transacao_detalhada':'na',
        'codigo_requisicao_transacao':'na',
        'chave_integracao_pgto_final':'na',
        'numero_pedido': orderid.split('|')[0]
      }, ignore_index=True)

  return response

# COMMAND ----------

# mari_milan20%40yahoo.com.br-89967a72-af16-46a0-9ae6-765748f647f3
# getOrdersDetails_paymentTransactionsEntries('123','mari_milan20@yahoo.com.br-89967a72-af16-46a0-9ae6-765748f647f3')
# getOrdersDetails_paymentTransactionsEntries('123','e1095ee0-0533-4b69-8d38-e861be7d2643%7Cemailtesteecomerceeveris3%40gmail.com-38ea255b-3d83-4c94-9530-71d8806a5826')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entries

# COMMAND ----------

async def getOrdersDetails_entries(orderid):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/entries"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }
  
  response = ''
  
  while response == '':
    if MODE == 'async':
    # if 1 == 0:
      transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
      async with httpx.AsyncClient(verify=False, transport=transport) as client:
        response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
    else:
      try:      
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
      except:
        print('Connection Refused - Orders Entries')
        print('Let me Sleep for 3 seconds')
        time.sleep(3)
  if response.status_code == 403:
  #if 1 == 1:
    print(response.status_code)
    print(url)
    print(response.text)
  #if response.status_code == 400:
  #  print(response.status_code)
  #  print(url)
  #  print(response.text)
  response = response.json()

  try:
    frame = pd.DataFrame.from_dict(response['d']['results'])

    if frame.empty:
      exit
    else:
      response = pd.DataFrame.from_dict(response['d']['results'])

      response['discountValuesInternal'] = response['discountValuesInternal'].map(lambda x: x.split('#')[1] if len(x) > 2 else 0.0)
      if orders_caracter != '':
        response['sku_produto'] = response['integrationKey'].map(lambda x: x.split('|')[4])
      else:
        response['sku_produto'] = response['integrationKey'].map(lambda x: x.split('|')[3])

      response.drop(['__metadata','product','order','quantityStatus'],axis=1, inplace=True)

      response['numero_pedido'] = orderid.split('|')[0]

      response.rename(columns={
        'totalPrice':'valor_total_sku',
        'quantity':'quantidade',
        'giveAway':'brinde',
        'VAEntryDiscount':'desconto_global',
        'discountValuesInternal':'desconto_interno',
        'basePrice':'preco_base',
        'integrationKey':'chave_integracao_produto'
      }, inplace=True)

      response[['valor_total_sku','desconto_global','desconto_interno','preco_base']] = response[['valor_total_sku','desconto_global','desconto_interno','preco_base']].astype(float)
      response['quantidade'] = response['quantidade'].astype(int)

      response['brinde'] = response['brinde'].map(lambda x: '1' if str(x) == 'True' else '0')

      brinde = []

      for i in response['brinde']:
        brinde.append(i)


      response['brinde'] = '|'.join(brinde)
      response = response.drop_duplicates()   
  except:
    response = pd.DataFrame()
    response = response.append({
      'valor_total_sku':'0',
      'quantidade':'0',
      'brinde':'na',
      'desconto_global':'0',
      'desconto_interno':'0',
      'preco_base':'0',
      'chave_integracao_produto':'na',
      'numero_pedido':orderid.split('|')[0]
    }, ignore_index=True)
  

  return response

# COMMAND ----------

# getOrdersDetails_entries('01753014')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product

# COMMAND ----------

async def getOrdersDetails_product(orderid, keyIntegration):
# def getOrdersDetails_product(keyIntegration):
  
  try:
    url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/OrderEntries('{keyIntegration}')/product"

    payload={}
    headers = {
      'Accept': 'application/json',
      'Authorization': f'Bearer {token["access_token"]}',
      "User-Agent": "insomnia/8.4.5",
      'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
    }

    response = ''
    while response == '':
      if MODE == 'async':
      # if 1 == 0:
        transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
        async with httpx.AsyncClient(verify=False, transport=transport) as client:
          response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
      else:
        try:
          response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        except:
          print('Connection Refused - Product')
          print('Let me Sleep for 3 seconds')
          time.sleep(3)
    if response.status_code == 403:
      print(response.status_code)
      print(url)
      print(response.text)
    response = response.json()

#     orderid = orderid

    frame = pd.DataFrame.from_dict(response['d'])

    if frame.empty:
      exit
    else:
      try:
        response = pd.DataFrame([response['d']])
      #   #drop columns dataframe
        response.drop(['__metadata','catalogVersion','supercategories','localizedAttributes'],axis=1, inplace=True)
        response.rename(columns={'code':'sku'}, inplace=True)
        response['numero_pedido_shopcode'] = orderid
        
        response.rename(columns={
          'ean':'ean',
          'name':'nome_produto',
          'commercialName':'nome_comercial',
          # 'sku':'sku_produto_detalhe',
          'sku':'sku_produto',
          'manufacturerAID':'codigo_fabricante',
          'manufacturerName':'nome_fabricante',
          'integrationKey':'chave_integracao_detalhe_produto'
        }, inplace=True)
      except:
        response = pd.DataFrame()
        response = response.append({
           'ean':'na',
           'nome_produto':'na',
           'nome_comercial':'na',
           # 'sku_produto_detalhe':'na',
           'sku_produto':'na',
           'codigo_fabricante':'na',
           'nome_fabricante':'na',
           'chave_integracao_detalhe_produto':'na',
           'numero_pedido_shopcode':orderid
        }, ignore_index=True)
  except:
    response = pd.DataFrame()
    response = response.append({
           'ean':'na',
           'nome_produto':'na',
           'nome_comercial':'na',
           # 'sku_produto_detalhe':'na',
           'sku_produto':'na',
           'codigo_fabricante':'na',
           'nome_fabricante':'na',
           'chave_integracao_detalhe_produto':'na',
           'numero_pedido_shopcode':orderid
        }, ignore_index=True)

  return response

# COMMAND ----------

# getOrdersDetails_product('Online|vivoProductCatalog|00897000|22018618')

# COMMAND ----------

# MAGIC %md
# MAGIC #### CatalogVersion

# COMMAND ----------

### %7C
def getOrdersDetails_productCatalogVersion(keyIntegration):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Products('{keyIntegration}')/catalogVersion"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload, verify=False)
  response = response.json()

  frame = pd.DataFrame.from_dict(response['d'])

  if frame.empty:
    exit
  else:
    response = pd.DataFrame([response['d']])
  #   #drop columns dataframe
    response.drop(['__metadata','catalog'],axis=1, inplace=True)

    response = normalizeDictOrders(response)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Catalog

# COMMAND ----------

def getOrdersDetails_productCatalog(keyIntegration):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/CatalogVersions('{keyIntegration}')/catalog"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload, verify=False)
  response = response.json()

  frame = pd.DataFrame.from_dict(response['d'])

  if frame.empty:
    exit
  else:
    response = pd.DataFrame([response['d']])
  #   #drop columns dataframe
    response.drop(['__metadata'],axis=1, inplace=True)

    response = normalizeDictOrders(response)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### Super Categories

# COMMAND ----------

def getOrdersDetails_productSuperCatalog(orderid, keyIntegration):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Products('{keyIntegration}')/supercategories"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()
  
  try:
    frame = pd.DataFrame.from_dict(response['d']['results'])

    if frame.empty:
      exit
    else:
      response = pd.DataFrame.from_dict(response['d']['results'])
      #drop columns dataframe
      response.drop(['__metadata','catalogVersion','localizedAttributes'], axis=1, inplace=True)

      category ={}
      count=1

      for cat in response['name']:
        category[f'categoria_nvl_{count}'] = cat
        count+=1

      response = pd.DataFrame([category])
      response['orderid'] = orderid.split('|')[0]
  except:
    response = pd.DataFrame(columns={'orderid':orderid.split('|')[0],'categoria_nvl_1':'NA'})
  return response

# COMMAND ----------

# getOrdersDetails_productSuperCatalog('123','Online|vivoProductCatalog|DGAP12213000')

# COMMAND ----------

def getOrdersDetails_productSuperCatalog_product(sku):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Products('Online|vivoProductCatalog|{sku}')/supercategories"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()
    
  try:
    frame = pd.DataFrame.from_dict(response['d']['results'])
    
    if frame.empty:
      exit
    else:
      response = pd.DataFrame.from_dict(response['d']['results'])
      #drop columns dataframe
      response.drop(['__metadata','catalogVersion','localizedAttributes'], axis=1, inplace=True)

      category ={}
      count=1

      for cat in response['name']:
        category[f'categoria_nvl_{count}'] = cat
        count+=1

      response = pd.DataFrame([category])
      
  except:
    response = pd.DataFrame(columns={'categoria_nvl_1':'NA'})
    
  return response

# COMMAND ----------

# getOrdersDetails_productSuperCatalog_product('DGAP05362000')

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEV#CatalogVersion
# MAGIC -- Standby

# COMMAND ----------

# ### %7C
# # def getOrdersDetails_superCatalogCatalogVersion(keyIntegration):
  
# url = f"https://{endpoint}/odata2webservices/OutboundOrder/Categories('Online%7CvivoProductCatalog%7Csmartphone')/catalogVersion"

# payload={}
# headers = {
#   'Accept': 'application/json',
#   'Authorization': f'Bearer {token["access_token"]}',
# }

# response = requests.request("GET", url, headers=headers, data=payload)
# response = response.json()

# #   frame = pd.DataFrame.from_dict(response['d'])

# #   if frame.empty:
# #     exit
# #   else:
# #     response = pd.DataFrame([response['d']])
# #   #   #drop columns dataframe
# #     response.drop(['__metadata','catalog'],axis=1, inplace=True)

# #     response = normalizeDictOrders(response)

# #   return response

# COMMAND ----------

# MAGIC %md
# MAGIC #### Localized Attributes Product

# COMMAND ----------

def getOrdersDetails_superCategoriesLocalizedAttributesProduct(keyIntegration):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Categories('{keyIntegration}')/localizedAttributes"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  frame = pd.DataFrame.from_dict(response['d']['results'])

  if frame.empty:
    exit
  else:
    response = pd.DataFrame.from_dict(response['d']['results'])
    #drop columns dataframe
    response.drop(['__metadata'],axis=1, inplace=True)

    response = normalizeDictOrders(response)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Status

# COMMAND ----------

# ## NIVEL PEDIDO

# # Consigments
#   ## Status de ordem de venda - SAP

# # Payments
#   ## Status de ordem de Pagamento
  
# # Status - Orders
#   ## Status de pedido

# ## Fluxo
# ##- Status Pedido - Pagamento - Consigments - Pedido
  
async def getOrdersDetails_status(orderid):
  
  url = f"https://{endpoint}/odata2webservices/OutboundOrderV2/Orders('{orderid}')/status"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
    "User-Agent": "insomnia/8.4.5",
    'cf-token': 'rgc9c833-f48o-437j-b4d9-f97d32ue3g88',
  }
  
  response = ''
  
  while response == '':
    if MODE == 'async':
      transport = httpx.AsyncHTTPTransport(retries=MAX_RETRIES)
      async with httpx.AsyncClient(verify=False, transport=transport) as client:
        response = await client.get(url, headers=headers, timeout=HTTPX_TIMEOUT)
    else:
      try:
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
      except:
        print('Connection Refused - Status')
        print('Let me Sleep for 3 seconds')
        time.sleep(3)
      
  if response.status_code == 403:
    print(response.status_code)
    print(url)
    print(response.text)
  response = response.json()

  try:
    del response['d']['__metadata']
    response = pd.DataFrame([response['d']])
    response['numero_pedido'] = orderid.split('|')[0]
    response.rename(columns={
      'code':'status_pedido',
      'integrationKey':'chave_integracao_status_pedido'
    }, inplace=True)
  except:
    response = pd.DataFrame()
    response = response.append({'status_pedido':'na','chave_integracao_status_pedido':'na','numero_pedido':orderid.split('|')[0]}, ignore_index=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ##Stock

# COMMAND ----------

def getStock():
  url = f"https://{endpoint}/odata2webservices/OutboundStock/StockLevels?$top=9999999"


  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])
  response['integrationKey'] = response['integrationKey'].map(lambda x: x.split('|')[1])
  response.drop(['__metadata','creationtime','warehouse','nextDeliveryTime'], axis=1, inplace=True)
  response.rename(columns={'integrationKey':'Warehouse'}, inplace=True)

  response = normalizeDictOrders(response)
  
  response['LVUT'] = (response['EstoqueVirtual']+response['EstoqueFisico'])-response['Reservado']
  
  response['DataCriacaoEstoque'] = pd.to_datetime(response['DataCriacaoEstoque'])
  
  return response

# COMMAND ----------

def getProductsSku(sku):

  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products?$filter=code eq '{sku}'&$top=1000"


  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])
  response['type'] = response['integrationKey'].map(lambda x: x.split('|')[0])
  response.drop(['__metadata','iframeURI','europe1Prices','supercategories',
                 'catalogVersion','barcodes','approvalStatus','videoList','galleryImages',
                 'classificationClasses','localizedAttributes','averageRating'],axis=1, inplace=True)

  response = response[response['type'] == 'Online']

  response.rename(columns={'name':'NomeProduto','code':'Sku','commercialName':'NomeComercial'}, inplace=True)

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## List all Products and Prices

# COMMAND ----------

def getAllProducts():

  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products?$top=9999999"


  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])
  response['type'] = response['integrationKey'].map(lambda x: x.split('|')[0])
  response.drop(['__metadata','iframeURI','supercategories',
                 'catalogVersion','barcodes','approvalStatus',
                 'videoList','galleryImages','classificationClasses',
                 'localizedAttributes'],axis=1, inplace=True)

  response = response[response['type'] == 'Online']

  response.rename(columns={'name':'NomeProduto','code':'Sku','commercialName':'NomeComercial','averageRating':'ClassificacaoMedia','integrationKey':'codigoIntegracao'}, inplace=True)
  

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### Barcode

# COMMAND ----------

def getBarcode(integrationkey):
  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('{integrationkey}')/barcodes"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()
  
  
  response = pd.DataFrame.from_dict(response['d']['results'])
  
  return results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approval Status

# COMMAND ----------

def getApprovalStatys(integratiokey):
  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('{integrationkey}')/approvalStatus"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  del response['d']['__metadata']

  response = pd.DataFrame([response['d']])

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEV#Picture
# MAGIC

# COMMAND ----------

# url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('Online%7CvivoProductCatalog%7CDGAP08262007')/picture"

# payload={}
# headers = {
#   'Accept': 'application/json',
#   'Authorization': f'Bearer {token["access_token"]}',
# }

# response = requests.request("GET", url, headers=headers, data=payload)
# response = response.json()

# response

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEV#Galery Images

# COMMAND ----------

# url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('Staged%7CvivoProductCatalog%7CDGAP08262007')/galleryImages"

# payload={}
# headers = {
#   'Accept': 'application/json',
#   'Authorization': f'Bearer {token["access_token"]}',
# }

# response = requests.request("GET", url, headers=headers, data=payload)
# response = response.json()

# # response = pd.DataFrame.from_dict(response['d']['results'])

# response

# #   return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prices

# COMMAND ----------

def getPrice(integrationkey):
  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('{integrationkey}')/europe1Prices"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])
  
  if response.empty:
     response = pd.DataFrame(columns=['PrecoFinal','Prioridade','price','DataFinal','DataInicio'])
  else:
    response.drop(['__metadata'], axis=1, inplace=True)
    response.rename(columns={'VAPriceEnd':'PrecoFinal','VAPriceStart':'PrecoInicial','startTime':'DataInicio','endTime':'DataFinal','priority':'Prioridade'}, inplace=True)
    response['Sku'] = integrationkey.split('|')[2]
    response = response[response['Prioridade'] == max(response['Prioridade'])]

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEV#Classification Classes - Details

# COMMAND ----------

def getClassificationProducts(sku):
  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('Online%7CvivoProductCatalog%7C{sku}')/classificationClasses"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload, verify=False)
  response = response.json()

  try:
    response = pd.DataFrame(response['d']['results'])
    response.rename(columns={'name':'classificacao'}, inplace=True)
  except:
    response = pd.DataFrame()
    response = response.append({'classificacao':'na'}, ignore_index=True)
  
  return response[['classificacao']]

# COMMAND ----------

# getClassificationProducts('22019051')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Videos

# COMMAND ----------

def getVideos(integrationkey):
  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('{integrationkey}')/videoList"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  response = response.json()

  response = pd.DataFrame.from_dict(response['d']['results'])
  response['sku'] = integrationkey.split('|')[2]

  return response

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classificacao SKU - Details

# COMMAND ----------

# def get_SkuDataSheet(sku):
  
#   matriz_details = ['modelo','codigofabricante','ean','marca']
# #   matriz_details = ['modelo']
#   classification = ['smartphone','assistentes','acessoriosGerais']
# #   classification = ['smartphone']
  
#   response_arr = {'marca':'', 'modelo':'','codigofabricante':'','ean':'', 'sku':sku,'classificacao':''}
  
#   for classif in classification:
    
#     for detail in matriz_details:
#       url = f"https://{endpoint}/odata2webservices/OutboundProduct/ProductFeatures('Online%7CvivoProductCatalog%7Cnull%7Cnull%7Cnull%7CvivoClassification%252F1.0%252F{classif}Classificacao.{detail}%7C{sku}')"

#       payload={}
#       headers = {
#         'Accept': 'application/json',
#         'Authorization': f'Bearer {token["access_token"]}',
#       }
      
#       response = ''
#       while response == '':
#         try:
#           response = requests.request("GET", url, headers=headers, data=payload)
#         except:
#           print('Connection Refused - Sku DataSheet')
#           print('Let me Sleep for 3 seconds')
#           time.sleep(3)
          
#       response = response.json()
      
#       try:
#         del response['d']['__metadata']
#         response = pd.DataFrame([response['d']])
#         response.drop(['integrationKey','unit','classificationAttributeAssignment','product','description','valueDetails'], axis=1, inplace=True)

#         response_arr[detail] = response['value']
#         response_arr['classificacao'] = 'acessorios' if classif == 'acessoriosGerais' else classif
        
#         response = pd.DataFrame(response_arr)
#       except:
#         try:
#           response = pd.DataFrame.from_dict(response_arr)
#         except:
#           response = pd.DataFrame.from_dict([response_arr])
#           response['classificacao'] = 'NA'

    
#   return response

# COMMAND ----------

def get_SkuDataSheet(sku):
  
  matriz_details = ['modelo','codigofabricante','ean','marca']
  classification = ['smartphone','assistentes','acessoriosGerais']
  
  response_arr = {'marca':'', 'modelo':'','codigofabricante':'','ean':'', 'sku':sku,'classificacao':''}
  
  url = f"https://{endpoint}/odata2webservices/OutboundProduct/Products('Online%7CvivoProductCatalog%7C{sku}')/features"

  payload={}
  headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token["access_token"]}',
  }
  
  response = ''
  while response == '':
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
    except:
      print('Connection Refused - Sku DataSheet')
      print('Let me Sleep for 3 seconds')
      time.sleep(3)
    
  response = response.json()

  try:
    response = pd.DataFrame(response['d']['results'])
  
    for classif in response['qualifier'].unique():
      classification = classif.split('/')[2]
      classification = classification.split('Classificacao')[0]
      response_arr['classificacao'] = classification
      
      classif_clean = classif.split('.')[2]

      if classif_clean in matriz_details:
        response_value = response[response['qualifier'] == classif]
        response_arr[classif_clean] = response_value.iloc[0]['value']

    response = pd.DataFrame([response_arr])
  except:
    try:
      response = pd.DataFrame.from_dict(response_arr)
    except:
      response = pd.DataFrame.from_dict([response_arr])
      response['classificacao'] = 'NA'

    
  return response

# COMMAND ----------

# MAGIC %md
# MAGIC # DEV#Run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Pedidos

# COMMAND ----------

# MAGIC %md
# MAGIC #### D-0

# COMMAND ----------

# start_date = subtract_days(7)
# end_date = subtract_days(1)
#async def get_pedidos_sap(semaphore, list_orders, order):
#
#  # acquire the semaphore
#  async with semaphore:  
#    # print(f'{order} - {datetime.now()}')
#    orders = list_orders[list_orders['numero_pedido_shopcode'] == order]
#
#    statusPedido = await getOrdersDetails_status(order)
#    qtdDias = await getOrder_deliverayDay(order)
#    order_user = await getOrdersDetails_user(order)
#    order_cupom = await getOrdersDetails_coupon(order)
#    order_paymants = await getOrdersDetails_paymentInfo(order)
#    order_paymants_transaction = await getOrdersDetails_paymentTransactions(order)
#    order_paymants_entries = await getOrdersDetails_paymentTransactionsEntries(order, order_paymants_transaction['codigo_transacao'][0])
#    order_list_product = await getOrdersDetails_entries(order)
#    sapOrder = await getOrdersDetails_consigments(order)
#    getlogin = await getVendor(order)
#    order_list_product_details = pd.DataFrame()
#    for key_integration in order_list_product['chave_integracao_produto']:
#      order_list_product_details_tmp = await getOrdersDetails_product(order, key_integration)
#      order_list_product_details = order_list_product_details.append(order_list_product_details_tmp)
#    # order_list_product_details = await getOrdersDetails_product(order_list_product['chave_integracao_produto'][0])
#
#    frame_orders = orders.merge(order_user, how='left', on='numero_pedido')
#
#    frame_orders = frame_orders.merge(order_paymants, how='left', on='numero_pedido')
#    
#    frame_orders = frame_orders.merge(order_paymants_transaction, how='left', on='numero_pedido')
#    frame_orders = frame_orders.merge(order_paymants_entries, how='left', on='numero_pedido')
#
#    frame_orders = frame_orders.merge(order_list_product, how='left', on='numero_pedido')
#
#    frame_orders = frame_orders.merge(statusPedido, how='left', on='numero_pedido')
#    frame_orders = frame_orders.merge(order_cupom, how='left', on='numero_pedido')
#    frame_orders = frame_orders.merge(sapOrder, how='left', on='numero_pedido')
#    
#    frame_orders = frame_orders.merge(qtdDias, how='left', on='numero_pedido')
#    frame_orders = frame_orders.merge(getlogin, how='left', on='numero_pedido')
#
#    if 'sku_produto' in frame_orders.columns and 'sku_produto' in order_list_product_details.columns:
#      frame_orders = frame_orders.merge(order_list_product_details, how='left', on=['numero_pedido_shopcode', 'sku_produto'])
#
#    else:
#      merged_list = frame_orders.merge(order_list_product_details, how='left', on=['numero_pedido_shopcode'])
#      result_list = merged_list.to_dict(orient='records')
#      print(result_list)
#
##    frame_orders = frame_orders.merge(order_list_product_details, how='left', on=['numero_pedido_shopcode', 'sku_produto'])
#
#    column_encrypt = ['email_ordem','cpf','telefone','celular']
#
#    for column in column_encrypt:
#      frame_orders[column] = frame_orders[column].map(lambda x: encrypt_string(x))
#
#    # print(f'    {order} - END:{datetime.now()}')
#    return frame_orders


async def get_pedidos_sap(semaphore, list_orders, order):
    async with semaphore:  
        orders = list_orders[list_orders['numero_pedido_shopcode'] == order]
        #print(order)
        # Recupera todas as informações
        statusPedido = await getOrdersDetails_status(order)
        qtdDias = await getOrder_deliverayDay(order)
        order_user = await getOrdersDetails_user(order)
        order_cupom = await getOrdersDetails_coupon(order)
        order_paymants = await getOrdersDetails_paymentInfo(order)
        order_paymants_transaction = await getOrdersDetails_paymentTransactions(order)
        order_paymants_entries = await getOrdersDetails_paymentTransactionsEntries(order, order_paymants_transaction['codigo_transacao'][0])
        order_list_product = await getOrdersDetails_entries(order)
        sapOrder = await getOrdersDetails_consignments(order)
        order_status_macro = await getOrdersDetails_consignmentsStatus(order, sapOrder['codigo_rastreio'][0])
        getlogin = await getVendor(order)
        order_sales_application = await getSalesApplication(order)
        order_delivery_status = await getDeliveryStatus(order)
        vivo_order_type = await getVivoOrderType(order)
        order_delivery_address = await getOrdersDetails_deliveryAddress(order)
        order_address_region = await getOrdersDetails_deliveryAddressRegion(order, order_delivery_address['region_id'][0])
        
        order_list_product_details = pd.DataFrame()
        for key_integration in order_list_product['chave_integracao_produto']:
            order_list_product_details_tmp = await getOrdersDetails_product(order, key_integration)
            order_list_product_details = order_list_product_details.append(order_list_product_details_tmp)
       
        # Iniciar com orders
        frame_orders = orders

        # Garantir que todas as variáveis sejam DataFrames antes do merge
        if isinstance(order_user, pd.DataFrame):
            frame_orders = frame_orders.merge(order_user, how='left', on='numero_pedido')

        if isinstance(order_paymants, pd.DataFrame):
            frame_orders = frame_orders.merge(order_paymants, how='left', on='numero_pedido')

        if isinstance(order_paymants_transaction, pd.DataFrame):
            frame_orders = frame_orders.merge(order_paymants_transaction, how='left', on='numero_pedido')

        if isinstance(order_paymants_entries, pd.DataFrame):
            frame_orders = frame_orders.merge(order_paymants_entries, how='left', on='numero_pedido')

        if isinstance(order_list_product, pd.DataFrame):
            frame_orders = frame_orders.merge(order_list_product, how='left', on='numero_pedido')

        if isinstance(statusPedido, pd.DataFrame):
            frame_orders = frame_orders.merge(statusPedido, how='left', on='numero_pedido')

        if isinstance(order_cupom, pd.DataFrame):
            frame_orders = frame_orders.merge(order_cupom, how='left', on='numero_pedido')

        if isinstance(sapOrder, pd.DataFrame):
            frame_orders = frame_orders.merge(sapOrder, how='left', on='numero_pedido')
        
        if isinstance(order_status_macro, pd.DataFrame):
            frame_orders = frame_orders.merge(order_status_macro, how='left', on='numero_pedido')

        if isinstance(qtdDias, pd.DataFrame):
            frame_orders = frame_orders.merge(qtdDias, how='left', on='numero_pedido')

        if isinstance(getlogin, pd.DataFrame):
            frame_orders = frame_orders.merge(getlogin, how='left', on='numero_pedido')

        if isinstance(order_sales_application, pd.DataFrame):
            frame_orders = frame_orders.merge(order_sales_application, how='left', on='numero_pedido')

        if isinstance(order_delivery_status, pd.DataFrame):
            frame_orders = frame_orders.merge(order_delivery_status, how='left', on='numero_pedido')
        
        if isinstance(vivo_order_type, pd.DataFrame):
            frame_orders = frame_orders.merge(vivo_order_type, how='left', on='numero_pedido')

        if isinstance(order_delivery_address, pd.DataFrame):
            frame_orders = frame_orders.merge(order_delivery_address, how='left', on='numero_pedido')
        
        if isinstance(order_address_region, pd.DataFrame):
            frame_orders = frame_orders.merge(order_address_region, how='left', on='numero_pedido')

        # Garantir que os detalhes do produto sejam mesclados corretamente
        if 'sku_produto' in frame_orders.columns and 'sku_produto' in order_list_product_details.columns:
            frame_orders = frame_orders.merge(order_list_product_details, how='left', on=['numero_pedido_shopcode', 'sku_produto'])
        else:
            print(f"Warning: produto ou sku não disponível para o pedido {order}, ignorando merge de produto.")

        # Encriptação dos campos sensíveis
        column_encrypt = ['email_ordem', 'cpf', 'telefone', 'celular']
        for column in column_encrypt:
            frame_orders[column] = frame_orders[column].map(lambda x: encrypt_string(x) if pd.notnull(x) else x)

        return frame_orders








###### Util #####
START_DATE = (datetime.now(timezone('America/Sao_Paulo')) - timedelta(days=30)).strftime('%Y-%m-%d') 
END_DATE = (datetime.now(timezone('America/Sao_Paulo')) - timedelta(days=1)).strftime('%Y-%m-%d') 

#START_DATE = '2025-02-14'
#END_DATE = '2025-02-15'

BQ_CLIENT = bigquery.Client(credentials=CREDENTIALS)

def now_local():
  return datetime.now(timezone('America/Sao_Paulo'))

def ingest_big_query(table, df, dataset='coe_datalake', schema=[]):
  dataset_ref = BQ_CLIENT.dataset(dataset)
  dataset_table = dataset_ref.table(table)

  settings_load = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    schema=schema
  )

  # API request
  query_job = BQ_CLIENT.load_table_from_dataframe(df, dataset_table, job_config=settings_load)

  # Waits for query to finish
  query_job.result()

# def get_orders_by_date_range(start_date, end_date):
#   list_orders = pd.DataFrame()
#   current_date = date.fromisoformat(start_date)

#   # loop request api for reprocess information
#   while current_date <= date.fromisoformat(end_date):
#     # date_start = str(current_date) + f'T00:00:00'
#     # date_end = str(current_date) + f'T23:59:59'

#     date_start = str(current_date) + f'T03:00:00'
#     date_end = str(current_date + timedelta(days=1)) + f'T02:59:59'
#     print(date_start + ' - ' + date_end)
    
#     _list_orders = getOrders(date_start, date_end)
#     list_orders = pd.concat([list_orders, _list_orders], ignore_index=True)
#     current_date = current_date + timedelta(days=1)

#   return list_orders

######################## NOVA FUNÇÃO get_orders_by_date ###########################
async def get_orders_in_batches(start_date, end_date, batch_size=3):
    current_date = date.fromisoformat(start_date)

    while current_date <= date.fromisoformat(end_date):
        batch_end_date = current_date + timedelta(days=batch_size - 1)
        if batch_end_date > date.fromisoformat(end_date):
            batch_end_date = date.fromisoformat(end_date)

        date_start = current_date.strftime('%Y-%m-%dT03:00:00')
        date_end = (batch_end_date + timedelta(days=1)).strftime('%Y-%m-%dT02:59:59')
        print(f"Processing batch: {date_start} - {date_end}")

        # Chamar a API para obter pedidos nesse intervalo
        list_orders = getOrders(date_start, date_end)
        yield list_orders  # Gerador para processar um batch por vez

        current_date = batch_end_date + timedelta(days=1)

## Função para deletar possíveis duplicados ##

def delete_duplicates():
  bigquery_client = bigquery.Client(credentials=CREDENTIALS)
  
  stmt = f"""
  DELETE FROM `telefonica-digitalsales.coe_datalake.sap_commerce_pedidos` WHERE uuid NOT IN (
    SELECT
      MAX(uuid) uuid
    FROM 
      `telefonica-digitalsales.coe_datalake.sap_commerce_pedidos`
    GROUP BY 
      numero_pedido, sku_produto, quantidade
  )
  """
  
  query_job = bigquery_client.query(stmt)
  #query_job.result()  # Aguarda a execução da query
  print("Duplicados deletados com sucesso!")

####### Função para deletar lotes que serão processados e evitar duplicidade #######

#def delete_lote(batch_dates):
#    bigquery_client = bigquery.Client(credentials=CREDENTIALS)
#    
#    # Converter as datas para strings formatadas
#    formatted_dates = ", ".join([f"'{date}'" for date in batch_dates])
#    
#    stmt = f"""
#    DELETE FROM `telefonica-digitalsales.coe_datalake.sap_commerce_pedidos`
#    WHERE DATE(data_criacao_pedido) IN ({formatted_dates})
#    """
    
#    query_job = bigquery_client.query(stmt)
#    #query_job.result()  # Aguarda a execução da query
#    print(f"Registros do lote {batch_dates} deletados com sucesso!")


###################### NOVA FUNÇÃO MAIN_SAP ###################################
async def main_sap_batches(start_date, end_date, batch_size=3):
  semaphore = asyncio.Semaphore(MAX_ASYNC_TASKS)
  dataframe = pd.DataFrame()
  failed_batches = []  # Para registrar os lotes com falhas

  # Processa os pedidos em batches  
  async for list_orders in get_orders_in_batches(start_date, end_date, batch_size):
    batch_dates = list_orders['data_criacao_pedido'].unique().tolist()#.head(1)  # Datas do lote atual
    retries = 0
    success = False

    while retries < 3 and not success:
      try:
        # Excluir registros do lote no BigQuery antes de reprocessar
        #delete_lote(batch_dates)

        tasks = [
          asyncio.create_task(get_pedidos_sap(semaphore, list_orders, order))
          for order in list_orders['numero_pedido_shopcode']
        ]

        values_list = await asyncio.gather(*tasks, return_exceptions=True)
        for value in values_list:
          if isinstance(value, Exception):
            print(f"Error processing order: {value}")
            raise value  # Força retry se houver erro
          else:
            dataframe = pd.concat([dataframe, pd.DataFrame(value)], ignore_index=True)
            #print('Processing order with success!')

        success = True  # Marca o lote como processado com sucesso
      except Exception as e:
        retries += 1
        print(f"Retrying batch {batch_dates}: {retries}/{3} retries.")

    if not success:
      print(f"Failed to process batch {batch_dates} after {3} retries.")
      failed_batches.append(batch_dates)

  # Adiciona UUID e data_request
  dataframe['uuid'] = dataframe['sku_produto'].map(lambda x: str(uuid.uuid4()))
  # dataframe['data_request'] = str(current_date)
  dataframe['data_request'] = datetime.now(timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')

  min_date = pd.to_datetime(dataframe['data_criacao_pedido']).min().strftime('%Y-%m-%d')
  max_date = pd.to_datetime(dataframe['data_criacao_pedido']).max().strftime('%Y-%m-%d')

  dataframe.rename(columns=str.lower, inplace=True)
  
  dataframe = dataframe[[
    'id_sistema',
    'uuid',
    'data_criacao_pedido',
    'data_request',
    'numero_pedido',
    'status_pedido',
    'cpf',
    'celular',
    'email_ordem',
    # 'classificacao',
    # 'nome_fabricante',
    # 'nome_produto',
    #  'ean',
    'sku_produto',
    'qtd_parcelas',
    'valor_parcela',
    'valor_total_sku',
    'bandeira_cartao',
    'valor_total_pedido',
    'total_desconto',
    'data_transacao_pgto',
    'fii_codigo',
    'fii_autorizacao',
    'fii_status',
    'token_requisicao',
    'provedor_pagamento',
    'status_transacao',
    'valor_transacao',
    'data_transacao_final',
    'status_transacao_detalhada',
    'brinde',
    'cupom_interno',
    'cupom_carrinho',
    'codigo_sap',
    'ov',
    'numero_serie_nf',
    'data_criacao_nf',
    'codigo_rastreio',
    'dias_entrega',
    'sap_msg',
    'quantidade',
    'preco_base',
    'shop_code',
    'login',
    'numero_nf',
    'nome_comercial',
    'sales_application',
    'asb_id',
    'delivery_status',
    'vivo_order_type',
    'regional',
    'status_macro'
  ]]

  #print('injestBigQuery DESABILITADO')
  #injestBigQuery('sap_commerce_pedidos', dataframe.astype(str))

  dataframe = dataframe.astype(str)

  # print(f'delete register: {date_delete}')
  deleteBigQuery(min_date, max_date)
  # print('delete register - Done')
  # print('deleteBigQuery DESABILITADO')
  #print(dataframe)
  injestBigQuery('sap_commerce_pedidos', dataframe)

  ### Grava lotes com falha em uma tabela ou arquivo ###
  if failed_batches:
    failed_df = pd.DataFrame({'failed_dates': [",".join(batch) for batch in failed_batches]})
    # Salva no BigQuery
    ingest_big_query('sap_commerce_lotes_erro', failed_df, schema=[
      bigquery.SchemaField('failed_dates', bigquery.enums.SqlTypeNames.STRING),
    ])

  return dataframe.shape[0], failed_batches
  

####### Main ########

log = {}

start_time = timeit.default_timer()
log['start_time'] = now_local().strftime('%Y-%m-%d %H:%M:%S')
log['report'] = f"Carga SAP D-30 - {now_local().strftime('%H:%M')}"

try:
    records, failed_batches = asyncio.run(
        main_sap_batches(START_DATE, END_DATE, batch_size=3)
    )

    
    print(f"Records processed: {records}")
    if failed_batches:
      print(f"Failed batches: {failed_batches}")

    delete_duplicates() 
    print(f"Records processed after delete duplicates: {records}")

    log['records'] = records
    log['failed'] = bool(failed_batches)
    log['error_message'] = None if not failed_batches else f"Failed batches: {failed_batches}"
except Exception as e:
    print(traceback.format_exc())
    log['records'] = 0 
    log['failed'] = True
    log['error_message'] = traceback.format_exc()

log['execution_time'] = timeit.default_timer() - start_time
print("Tempo de execução:", timeit.default_timer() - start_time)

df_log = pd.DataFrame(log, index=[0])
df_log['start_time'] = pd.to_datetime(df_log['start_time'])

ingest_big_query('sap_commerce_rt_logs', df_log, schema=[
    bigquery.SchemaField('start_time', bigquery.enums.SqlTypeNames.DATETIME),
    bigquery.SchemaField('error_message', bigquery.enums.SqlTypeNames.STRING),
])
