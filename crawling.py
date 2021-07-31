# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 10:12:33 2021

@author: MJH
"""

import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup


class crawling:
    
    press_code_dictionary = {
        '032': '경향신문',        
        '005': '국민일보',
        '020': '동아일보',
        '021': '문화일보',
        '081': '서울신문',
        '022': '세계일보',
        '023': '조선일보',
        '025': '중앙일보',
        '028': '한겨레',
        '469': '한국일보',
        '421': '뉴스1',
        '003': '뉴시스',
        '001': '연합뉴스',
        '422': '연합뉴스TV',
        '449': '채널A',
        '215': '한국경제TV',
        '437': 'JTBC',
        '056': 'KBS',
        '214': 'MBC',
        '057': 'MBN',
        '055': 'SBS',
        '374': 'SBS Biz',
        '448': 'TV조선',
        '052': 'YTN',
        '009': '매일경제',
        '008': '머니투데이',
        '648': '비즈니스워치',
        '011': '서울경제',
        '277': '아시아경제',
        '018': '이데일리',
        '366': '조선비즈',
        '123': '조세일보',
        '014': '파이낸셜뉴스',
        '015': '한국경제',
        '016': '헤럴드경제',
        '079': '노컷뉴스',
        '629': '더팩트',
        '119': '데일리안',
        '417': '머니S',
        '006': '미디어오늘',
        '031': '아이뉴스24',
        '047': '오마이뉴스',
        '002': '프레시안',
        '138': '디지털데일리',
        '029': '디지털타임스',
        '293': '블로터',
        '030': '전자신문',
        '092': 'ZDNet Korea',
        '145': '레이디경향',
        '024': '매경이코노미',
        '308': '시사IN',
        '586': '시사저널',
        '262': '신동아',
        '094': '월간 산',
        '243': '이코노미스트',
        '033': '주간경향',
        '037': '주간동아',
        '053': '주간조선',
        '353': '중앙SUNDAY',
        '036': '한겨레21',
        '050': '한경비즈니스',
        '127': '기자협회보',
        '607': '뉴스타파',
        '584': '동아사이언스',
        '310': '여성신문',
        '007': '일다',
        '640': '코리아중앙데일리',
        '044': '코리아헤럴드',
        '296': '코메디닷컴',
        '346': '헬스조선',
        '087': '강원일보',
        '088': '매일신문',
        '082': '부산일보',
        '348': '신화사 연합뉴스',
        '077': 'AP연합뉴스',
        '091': 'EPA연합뉴스'
        }
    
    
    async def _getPageRangeDictionary(self, press_code, date):
        '''
        
        Parameters
        ----------
        press_code : int
            press code
        date : str
            'yyyymmdd'

        Returns
        -------
        page_range_dictionary : list
        [{'press_code': '023', 'date': '20210120', 'page_range': range(1, 13)}]
            
        '''
        url = f'https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec&oid={press_code}&date={date}&page=10000'
        

        async with aiohttp.ClientSession() as sess:
            async with sess.get(url,
                                headers = {'user-agent': 'Mozilla/5.0'}) as response:
                try:    
                    text = await response.text()
                except UnicodeDecodeError:
                    text = await response.text(encoding = 'cp949')
              
        bs_response = BeautifulSoup(text, 'lxml')

        try:
            last_page_code = bs_response.select('div.paging a')[-1]['href']
            last_page = re.findall('page=(\d+)', last_page_code)[0]
        except:
            try:
                last_page_code = bs_response.select('div.paging strong')[-1]
                last_page = last_page_code.text
            except:    
                last_page = -1
            
        page_range_dictionary = {
            "press_code": press_code,
            "date": date,
            "page_range": range(1, int(last_page) + 1) if last_page != -1 else -1
            }
        
        return page_range_dictionary
                
    
    async def getPageRangeDictionary(self, date_lists):
            
        futures = [asyncio.ensure_future(self._getPageRangeDictionary(press_code, date)) for date in date_lists for press_code in self.press_code_dictionary.keys()]
        
        return await asyncio.gather(*futures)
    
    
    def _getURLLists(self, page_range_dictionary):
        
        url_lists = []
        for row in page_range_dictionary:
            if row['page_range'] != -1:
                for page in row['page_range']:
                    url = f"https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec&oid={row.get('press_code')}&date={row.get('date')}&page={page}"
                    url_lists.append(url)
        
        return url_lists
    
    
    async def _getArticleURLs(self, url):
                
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url,
                                    headers = {'user-agent': 'Mozilla/5.0'}) as response:
                    text = await response.text()
                    bs_response = BeautifulSoup(text, 'lxml')

            selected_bs_response = bs_response.select('dt a')
            unique_article_urls = set(i['href'] for i in selected_bs_response)
            
            print(f'complete collecting {url}')

            return unique_article_urls
        
        except:
            return await self._getArticleURLs(url)
      
        
    async def getArticleURLs(self, page_range_dictionary):
        
        url_lists = self._getURLLists(page_range_dictionary)
        
        futures = [asyncio.ensure_future(self._getArticleURLs(url)) for url in url_lists]
        
        return await asyncio.gather(*futures)
    
    
    def _getArticleTime(self, response, time):
        '''        
        Parameters
        ----------
        response : bs4
        values from response.
        time : integer
        0: input, 1: modify.
            
        Returns
        -------
        TYPE
        DESCRIPTION.

        '''
        try:
            return response.select('div.info span')[time].text
        except:
            try:    
                return response.select('span.t11')[time].text
            except: 
                try:
                    return response.select('div.article_info span')[time].text
                except:
                    try:
                        return response.select('div.sponsor span')[time].text
                    except:
                        return ''
                    
                    
    def _get_sid(self, main_content):
        '''        

        Parameters
        ----------
        main_content : BeautifulSoup object
            beautifulSoup object got from _get_main_content

        Returns
        -------
        sid : str
            category number   

        '''
        
        reading_observer = list(filter(lambda x: 'ReadingObserver' and 'sid' in str(x), main_content.select('script')))[0]
        sid = re.findall('sid:\s?(.+),', str(reading_observer))[0].replace("'", '')
        
        return sid
        

                    
    async def _get_data(self, url):
        
        connector = aiohttp.TCPConnector(limit = 60)
        async with aiohttp.ClientSession(connector = connector) as sess:
            async with sess.get(url,
                                headers = {'user-agent': 'Mozilla/5.0'}) as response:
                try:    
                    text = await response.text()
                except UnicodeDecodeError:
                    text = await response.text(encoding = 'cp949')
                    
                main_content = BeautifulSoup(text, 'lxml')
        
                if main_content.select('div.error'):
                    return ''
                
                else:
                   press_code = re.findall('oid=(\d{3})', url)[0]
                   try:                        
                       data = [{
                               'press_name': self.press_code_dictionary[press_code],
                               'press_code': press_code,
                               'category': self._get_sid(main_content),
                               'title': str(main_content.select('h3#articleTitle')[0]).replace("'", "''"),
                               'contents': str(main_content.select('div#articleBodyContents')[0]).replace("'", "''"),
                               'content_original': str(main_content.select('.content')[0]).replace("'", "''"),
                               'url': url,
                               'date_upload': self._getArticleTime(main_content, 0),
                               'date_modify': self._getArticleTime(main_content, 1)
                               }]
                   
                   except:
                       try:
                           data = [{                               
                               'press_name': self.press_code_dictionary[press_code],
                               'press_code': press_code,
                               'category': self._get_sid(main_content),
                               'title': str(main_content.select('h4.title')[0]).replace("'", "''"),
                               'contents': str(main_content.select('div#newsEndContents')[0]).replace("'", "''"),
                               'content_original': str(main_content.select('.content')[0]).replace("'", "''"),
                               'url': url,
                               'date_upload': self._getArticleTime(main_content, 0),
                               'date_modify': self._getArticleTime(main_content, 1)
                               }]
                            
                       except:
                           try:
                               data = [{
                                   'press_name': self.press_code_dictionary[press_code],
                                   'press_code': press_code,
                                   'category': self._get_sid(main_content),
                                   'title': str(main_content.select('h2.end_tit')[0]).replace("'", "''"),
                                   'contents': str(main_content.select('div#articeBody')[0]).replace("'", "''"),
                                   'content_original': str(main_content.select('.content')[0]).replace("'", "''"),
                                   'url': url,
                                   'date_upload': self._getArticleTime(main_content, 0),
                                   'date_modify': self._getArticleTime(main_content, 1)
                                   }]
                           except:
                               return 
                                
                   print(f'complete getting data from {url}')
                   return data




    async def get_data(self, urls):
     
        futures = [asyncio.ensure_future(self._get_data(url)) for url in urls]
        
        return await asyncio.gather(*futures)
