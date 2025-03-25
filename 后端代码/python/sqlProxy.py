import random
import threading
import time
from datetime import datetime
import psycopg2


# base class for sql operation
class SQLProxy:
    def __init__(self, host, user, password, db, port, charset):
        try:
            self.conn = psycopg2.connect(host=host, user=user, password=password,
                                         database=db, port=port)
            self.cursor = self.conn.cursor()
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)
        except Exception as e:
            self.rollback()
            print(e)
            exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def rollback(self):
        self.conn.rollback()

    def set_auto_commit(self, flag):
        self.conn.autocommit = flag

    # no exception handle
    def no_exception_handle(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.rollback()
            print(e)
            return None

    # execute sql and return the result
    def __execute_and_return(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    # execute sql but return nothing
    # the caller can use cursor to get the result
    def __execute_only(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self.cursor

    def get_schema(self, table: str):
        def _getSchema():
            self.cursor.execute(f'select * from {table};')
            return self.cursor.description

        return self.no_exception_handle(_getSchema)

    def get_tables(self):
        def _getTables():
            self.cursor.execute('show tables')
            return self.cursor.fetchall()

        return self.no_exception_handle(_getTables)

    def clear_table(self, table: str):
        def _clearTable():
            print(f'正在清空表{table}, 是否继续?(yes/no)')
            answer = input()
            if answer.lower() != 'yes':
                return None
            self.cursor.execute(f'delete from {table} where true;')

        return self.no_exception_handle(_clearTable)

    def insert_data(self, table: str, data: dict):
        def _insertData():
            keys = ', '.join(data.keys())
            values = [f'{value}' for value in data.values()]
            sLen = '%s, ' * (len(values) - 1) + '%s'
            query = f'INSERT INTO {table} ({keys}) VALUES ({sLen});'
            self.cursor.execute(query, tuple(values))

        return self.no_exception_handle(_insertData)

    def insert_data_list(self, table: str, data: list):
        def _insertDataList():
            keys = self.get_schema(table)
            sLen = '%s, ' * (len(keys) - 1) + '%s'
            keys = ', '.join([key[0] for key in keys])
            query = f'insert into {table} ({keys}) values ({sLen});'

            self.cursor.executemany(query, data)

        return self.no_exception_handle(_insertDataList)

    def create_index(self, table: str, index: str):
        def _createIndex():
            self.cursor.execute(f'create index {index} on {table}({index})')

        return self.no_exception_handle(_createIndex)

    def create_index_for_all_primary_key(self):
        tables = self.get_tables()
        for table in tables:
            schema = self.get_schema(table[0])
            for column in schema:
                if column[3] == 'PRI':
                    self.create_index(table[0], column[0])

    def show_index(self, table: str):
        def _showIndex():
            self.cursor.execute(f'show index from {table}')
            return self.cursor.fetchall()

        return self.no_exception_handle(_showIndex)

    def safe_execute_and_return(self, sql: str, params: list = None) -> list:
        res = self.no_exception_handle(self.__execute_and_return, sql, params)
        if res is None:
            return []
        return res

    def safe_execute_only(self, sql: str, params: list = None):
        return self.no_exception_handle(self.__execute_only, sql, params)

    def commit(self):
        self.conn.commit()

    def create_keep_live_interval(self):
        def heart_beat():
            while True:
                self.__execute_only('select 1')
                time.sleep(60)

        t = threading.Thread(heart_beat())
        t.setDaemon(True)
        t.start()


# subclass for Project table sql operation
# Ex: get all reviews of any people by username
# get_review(username='a')
class ProjectSQLProxy(SQLProxy):
    def __init__(self, host='localhost', user='root', password='root', db='Project', port=53306, charset='utf8'):
        super().__init__(host, user, password, db, port, charset)

    """ query operations get data """

    def get_service_from_tip(self, tipid: str):
        """get the service referred by some tip, if not exist return None

        Args:
            tipid (str): the id of the tip

        Returns:
            str: the id of the business referred by the tip
        """
        res = self.safe_execute_and_return("select businessid from tip where tipid=%s", [tipid])
        if len(res) == 0:
            return None
        return res[0][2]

    def get_userid_by_account_number(self, account: str):
        """
        get userid by account number, if not exist return None
        :param account: user's account number
        :type account: str
        :return: user's userid
        :rtype: str
        """
        res = self.safe_execute_and_return("select userid from \"account\" where account=%s;", [account])
        if len(res) == 0:
            return None
        return res[0][0]

    def get_username_by_userid(self, userid: str):
        """
        get user's username by userid, if not exist return None
        :param userid: user's userid
        :type userid: str
        :return: user's username
        :rtype: str
        """
        res = self.safe_execute_and_return("select username from \"user\" where userid=%s;", [userid])
        if len(res) == 0:
            return None
        return res[0][0]

    def get_userInfo(self, userid: str):
        """
        get user's information by userid, if not exist return None
        :param userid: user's userid
        :type userid: str
        :return: someone's information, including userid, name, reviewNum, avatar
        :rtype: dict
        """
        res = self.safe_execute_and_return("select * from \"user\" where userid=%s;", [userid])
        if len(res) == 0:
            return None
        ret_data = {'userid': res[0][0],
                    'name': res[0][1],
                    'reviewNum': res[0][2],
                    'avatar': f"http://124.70.185.215:1414/avatars/{self.get_avatar_by_userid(res[0][0])}.png"}
        return ret_data

    def get_avatar_by_userid(self, userid: str):
        """
        get user's avatar by userid, if not exist return None
        :param userid: user's userid
        :type userid: str
        :return: user's avatar filename, for http str
        :rtype: str
        """
        res = self.safe_execute_and_return("select avatar from \"useravatar\" where userid=%s;", [userid])
        if len(res) == 0:
            return None
        return res[0][0]

    def get_service_info(self, businessid: str):
        """
        get service's information by businessid, if not exist return None
        :param businessid: one service's businessid
        :type businessid: str
        :return: service's information, including businessid,
                 name, address, score, review_num, tags, isopen, openhours
        :rtype: dict
        """
        res = self.safe_execute_and_return("select * from business where businessid=%s;", [businessid])
        data = {'businessid': res[0][0], 'name': res[0][1], 'address': res[0][2],
                'score': 5.0 if res[0][9] == 0 else res[0][8] / res[0][9], 'review_num': res[0][9],
                'tags': res[0][11].split(', '), 'isopen': res[0][10], 'openhours': res[0][12]}
        return data

    def get_businessname_by_businessid(self, businessid: str):
        """
        get businessname by businessid, if not exist return None
        :param businessid: one service's businessid
        :type businessid: str
        :return: this service's name
        :rtype: str
        """
        res = self.safe_execute_and_return("select businessname from business where businessid=%s;", [businessid])
        if len(res) == 0:
            return None
        return res[0][0]

    def get_review_by_businessid(self, businessid: str):
        """
        get all reviews of any business
        :param businessid: one service's businessid
        :type businessid: str
        :return: list of reviews (reviewid, userid, businessid, totalScore, date, text, useful, funny, cool)
        :rtype: list
        """
        res = self.safe_execute_and_return("select * from review where businessid=%s;", [businessid])
        return list(res)

    def get_friends(self, **kwargs):
        """
        get all friends of any people by username or userid, if userid not exist return []
        :param kwargs:
        :type kwargs: dict
        :return: list of friends' userid
        :rtype: list
        """
        if len(kwargs) != 1 or 'userid' not in kwargs:
            return []

        def splitFriend(friend):
            """
            split the friends string into a list
            :param friend: a string of friends
            :type friend: str
            :return: a list of friends
            :rtype: list
            """
            lis = friend.split(',')
            for idx in range(len(lis)):
                lis[idx] = lis[idx].strip()
            return lis

        userid = kwargs['userid']
        res = self.safe_execute_and_return("select secondUserID from friends where firstuserid=%s;", [userid])
        for i in range(len(res)):
            res[i] = splitFriend(res[i][0])
        res = [item for sublist in res for item in sublist]
        return res

    def get_recommends_business(self, **kwargs):
        """
        get all business which a user don't review but his friends review
        :param kwargs: userid-the user's userid,
                       count-the number of recommend business
        :type kwargs: dict
        :return: list of business (businessid, totalScore)
        :rtype: list
        """
        if 'userid' in kwargs:
            userid = kwargs['userid']
        else:
            return []

        max_limit = 10
        if 'count' in kwargs:
            max_limit = kwargs['count']

        friends = self.get_friends(userid=userid)
        business = []
        if len(friends) != 0:
            friends = tuple(friends)
            # get all business which a user don't review
            sql = """
                SELECT distinct businessid, avg(score) as avg_score
                FROM review
                WHERE userid in %s
                AND businessid NOT IN (
                    SELECT businessid
                    FROM \"recommended\"
                    WHERE userid = %s
                )
                AND businessid IN (
                    SELECT businessid
                    FROM photo
                )
                GROUP BY businessid
                ORDER BY avg_score desc
                limit %s;
                """
            res = self.safe_execute_and_return(sql, [friends, userid, max_limit])
            if res is not None:
                business += [item for item in res]

        if len(business) < max_limit:  # if the user has no friends randomly select some business
            sql = ("SELECT businessid, totalscore FROM business "
                   "WHERE businessid not in "
                   "(SELECT businessid FROM recommended WHERE userid = %s) "
                   "and businessid in (SELECT businessid FROM photo) "
                   "ORDER BY RANDOM() LIMIT %s;")
            res = self.safe_execute_and_return(sql, [userid, max_limit - len(business)])
            if res is not None:
                for item in res:
                    if len(business) >= max_limit:
                        break
                    business.append(item)

        for commend in business:
            self.insert_data('recommended', {'userid': userid, 'businessid': commend[0]})
        self.commit()

        return business

    def get_recommends_tips(self, **kwargs):
        """
        get all recommend tips
        :param kwargs: userid:the user's userid,count:the number of recommend tips
        :type kwargs: dict
        :return: list of business (tipid, compliment_count)
        :rtype: list
        """
        if 'userid' in kwargs:
            userid = kwargs['userid']
        else:
            return []

        max_limit = 10
        if 'count' in kwargs:
            max_limit = kwargs['count']

        sql = 'select tipid, complimentcount from tip order by complimentcount desc limit %s'
        res = self.safe_execute_and_return(sql, [max_limit * 10])
        res = random.choices(res, k=max_limit)
        return res

    def get_tip_info(self, tipid: str):
        """
        get all tip info by tipid
        :param tipid: one tip's tipid
        :type tipid: str
        :return: information of business (tipid, userid, businessid, text, compliment_count, date)
        :rtype: dict
        """
        res = self.safe_execute_and_return("select * from tip where tipid=%s;", [tipid])
        if len(res) == 0:
            return dict()
        ret_data = {
            'tipid': res[0][0],
            'userid': res[0][1],
            'businessid': res[0][2],
            'text': res[0][3],
            'compliment_count': res[0][4],
            'date': res[0][5].strftime("%Y-%m-%d %H:%M:%S")
        }
        return ret_data

    def get_post_info(self, tipid: str):
        """
        get all post info by tipid
        :param tipid: one tip's tipid
        :type tipid: str
        :return: information of post (postid, userid, businessid, text, date, image, tags, name, avatar)
        :rtype: dict
        """
        temp_data = self.get_tip_info(tipid)
        # get picture
        picInfo = self.get_picture(businessid=temp_data['businessid'])
        temp_data['image'] = []
        if picInfo is not None:
            for info in picInfo:
                temp_data['image'].append(f"http://124.70.185.215:1414/servicePictures/{info[0]}.jpg")

        # get tags
        businessInfo = self.get_service_info(temp_data['businessid'])
        temp_data['tags'] = businessInfo['tags']

        # get username
        temp_data['name'] = self.get_username_by_userid(userid=temp_data['userid'])

        # get user avatar
        avatarInfo = self.get_avatar_by_userid(userid=temp_data['userid'])
        if avatarInfo is not None:
            temp_data['avatar'] = f"http://124.70.185.215:1414/avatars/{avatarInfo}.png"
        else:
            temp_data['avatar'] = 'null'

        return temp_data

    def _getLike(self, superlike_cursor, start):
        if start >= superlike_cursor.rowcount:
            return []
        superlike_cursor.scroll(start, mode='absolute')
        superlike = superlike_cursor.fetchmany(size=10)
        ret_data = []
        for row in superlike:
            ret_data.append(row[0])
        return ret_data

    def get_all_user_superlike(self, userid: str):
        """
        get someone's superlike business by someone's userid
        :param userid: user's userid of the user
        :type userid: str
        :return: list of user's superlike business infomation (businessid, name, address, score, review_num, tags, runtime)
        :rtype: list
        """
        superlike_business = self.safe_execute_and_return("select * from business where businessid in "
                                                          "(select businessid from superlike where userid=%s);",
                                                          [userid])
        ret_data = []
        for business in superlike_business:
            ret_data.append(
                {'businessid': business[0], 'name': business[1], 'address': business[2],
                 'score': 5.0 if business[9] == 0 else business[8] / business[9],
                 'review_num': business[9], 'tags': business[11].split(', '),
                 'runtime': business[12]})

            # get picture
            picInfo = self.get_picture(businessid=business[0])
            ret_data[-1]['image'] = []
            if picInfo is not None:
                for info in picInfo:
                    ret_data[-1]['image'].append(f"http://124.70.185.215:1414/servicePictures/{info[0]}.jpg")

        return ret_data

    def get_user_superlike(self, userid: str, start: int):
        """
        get someone's superlike business by someone's userid
        :param userid: user's userid
        :type userid: str
        :param start: start index of the result
        :type start: int
        :return: list of business (businessid)
        :rtype: list
        """
        cursor = self.safe_execute_only("select businessid from superlike where userid=%s;", [userid])
        return self._getLike(cursor, start)

    def get_user_like(self, userid: str, start: int):
        """
        get someone's like business by someone's userid
        :param userid: user's userid
        :type userid: str
        :param start: start index of the result
        :type start: int
        :return: list of business (businessid)
        :rtype: list
        """
        cursor = self.safe_execute_only("select businessid from likes where userid=%s;", [userid])
        return self._getLike(cursor, start)

    # get picture by businessid
    # input: businessid
    # output: photoinfo (photoid, businessid, caption, label)
    def get_picture(self, businessid: str):
        """
        get picture by businessid
        :param businessid: service's businessid
        :type businessid: str
        :return: photo info (photoid, businessid, caption, label)
        :rtype: list
        """
        res = self.safe_execute_and_return("select * from photo where businessid=%s;", [businessid])
        return res

    def _getReviews(self, reviews_cursor, start):
        """
        get the reviews of a user or a business
        :param reviews_cursor: review cursor
        :type reviews_cursor:
        :param start: start index of the result
        :type start: int
        :return: list of reviews (reviewid, userid, businessid, totalScore, date, text, useful, funny, cool)
        :rtype: list
        """
        if start >= reviews_cursor.rowcount:
            return []
        reviews_cursor.scroll(start, mode='absolute')
        reviews = reviews_cursor.fetchmany(size=10)

        if reviews is None or len(reviews) == 0:
            return []

        ret_data = []
        for review in reviews:
            ret_data.append({'reviewid': review[0], 'score': review[7], 'date': review[3], 'text': review[8],
                             'useful': review[4], 'funny': review[5], 'cool': review[6]})
            ret_data[-1]['username'] = self.get_username_by_userid(review[1])
            ret_data[-1]['userid'] = review[1]
            ret_data[-1]['businessname'] = self.get_businessname_by_businessid(review[2])
            # convert datetime to string
            ret_data[-1]['date'] = ret_data[-1]['date'].strftime('%Y-%m-%d %H:%M:%S')
            # get user avatar
            avatarInfo = self.get_avatar_by_userid(userid=ret_data[-1]['userid'])
            if avatarInfo is not None:
                ret_data[-1]['avatar'] = f"http://124.70.185.215:1414/avatars/{avatarInfo}.png"  # http://124.70.185.215:1414/
            else:
                ret_data[-1]['avatar'] = 'null'

        return ret_data

    def _getPosts(self, tips_cursor, start):
        if start >= tips_cursor.rowcount:
            return []
        tips_cursor.scroll(start, mode='absolute')
        tips = tips_cursor.fetchmany(size=10)
        ret_data = []
        for row in tips:
            ret_data.append(row[0])
        return ret_data

    def get_reviews_by_businessid(self, businessid: str, start: int):
        """
        get the reviews of a business
        :param businessid: service's businessid
        :type businessid: str
        :param start: start index of the result
        :type start: int
        :return: list of reviews (reviewid, userid, businessid, totalScore, date, text, useful, funny, cool)
        :rtype: list
        """
        cursor = self.safe_execute_only("select * from review where businessid=%s order by date desc;", [businessid])
        return self._getReviews(cursor, start)

    def get_replies(self, reviewid: str, start: int):
        """
        get the replies of a review
        :param reviewid: the review's reviewid to get the replies
        :type reviewid: str
        :param start: start index of the result
        :type start: int
        :return: list of replies (reviewid, userid, businessid, totalScore, date, text, useful, funny, cool)
        :rtype: list
        """
        cursor = self.safe_execute_only("select * from reply where reviewid=%s order by date desc;",
                                        [reviewid])
        return self._getReviews(cursor, start)

    def get_reviews_by_userid(self, userid: str, start: int):
        """
        get the reviews of a user by userid
        :param userid: user's userid
        :type userid: str
        :param start: star index of the result
        :type start: int
        :return: list of reviews (reviewid, userid, businessid, totalScore, date, text, useful, funny, cool)
        :rtype: list
        """
        cursor = self.safe_execute_only("select * from review where userid=%s order by date desc;", [userid])
        return self._getReviews(cursor, start)

    def get_tips_by_businessid(self, businessid: str, start: int):
        """
        get the relative posts of a service
        :param businessid: the service's businessid
        :type businessid: str
        :param start: start index of the result
        :type start: int
        :return: list of tips (tipid)
        :rtype: list
        """
        cursor = self.safe_execute_only("select tipid from tip where businessid=%s order by date desc;", [businessid])
        return self._getPosts(cursor, start)

    def get_posts_by_userid(self, userid: str, start: int):
        """
        get this relative posts of a user by userid
        :param userid: the user's userid
        :type userid: str
        :param start: start index of the result
        :type start: int
        :return: list of tips (tipid)
        :rtype: list
        """
        cursor = self.safe_execute_only("select tipid from tip where userid=%s order by date desc;", [userid])
        return self._getPosts(cursor, start)

    def get_all_user(self):
        """
        get all user
        :return: list of user (userid, username, review_count, yelping_since, useful, funny, cool, fans, average_stars)
        :rtype: list
        """
        res = self.safe_execute_and_return("select * from \"user\";")
        return res

    def get_wantGos_by_businessid(self, businessid):
        """
        get want gos by businessid
        :param businessid: the service's businessid to get the want gos
        :type businessid: str
        :return: list of user (userid)
        :rtype: list
        """
        res = self.safe_execute_and_return("select * from \"wantgo\" where businessid=%s;", [businessid])
        users = []
        for (user, _) in res:
            users.append(user)
        return users

    def get_wantGos_by_userid(self, userid):
        """
        get someone's want gos by userid
        :param userid: the user's userid to get the want gos
        :type userid: str
        :return: list of service (businessid)
        :rtype: list
        """
        res = self.safe_execute_and_return("select * from \"wantgo\" where userid=%s;", [userid])
        return res

    def get_unreceptive_chat(self, userid: str):
        """
         get the chat record sent when the user can't recept
        :param userid: the user's userid
        :type userid: str
        :return: list of chat record (chatid, firstuserid, seconduserid, context, contexttype, isfirsttosecond, timestamp)
        :rtype: list
        """
        res = self.safe_execute_and_return("select * from \"userchat\" where "
                                           "(chatid in (select chatid from chatreceipt where isrecept=0))"
                                           "and ((firstuserid=%s and isfirsttosecond=0) or (seconduserid=%s and isfirsttosecond=1));",
                                           [userid, userid])

        ret_data = []
        for (chatid, firstuserid, seconduserid, context, contexttype, isfirsttosecond, timestamp) in res:
            ret_data.append({
                'chatid': chatid, 'firstuserid': firstuserid, 'seconduserid': seconduserid,
                'isfirsttosecond': isfirsttosecond, 'context': context,
                'contexttype': contexttype, 'time': timestamp.timestamp()
            })
        return ret_data

    """ query operations exist or not """

    def is_username_exist(self, username: str):
        """
        judge whether a username is in the database
        :param username: user's username
        :type username: str
        :return: is username exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from \"user\" where username=%s;", [username])
        return len(res)

    def is_account_exist(self, account: str):
        """
        judge whether an account is in the database
        :param account: user's account
        :type account: str
        :return: is account exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from \"account\" where account=%s;", [account])
        return len(res)

    def is_login_success(self, username: str, password: str):
        """
        judge whether a pair of username and password is in the database
        :param username: user's username
        :type username: str
        :param password: user's password
        :type password: str
        :return: is login success or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from account where account=%s and password=%s;",
                                           [username, password])
        return len(res)

    def is_userid_exist(self, userid: str):
        """
        judge whether an userid is in the database
        :param userid: user's userid
        :type userid: str
        :return: is userid exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from \"user\" where userid=%s;", [userid])
        return len(res)

    def is_business_exist(self, businessid: str):
        """
        judge whether a businessid is in the database
        :param businessid: a service's businessid
        :type businessid: str
        :return: is businessid exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from business where businessid=%s;", [businessid])
        return len(res)

    def is_review_exist(self, reviewid: str):
        """
        judge whether a reviewid is in the database
        :param reviewid: the review's reviewid
        :type reviewid: str
        :return: is reviewid exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from review where reviewid=%s;", [reviewid])
        return len(res)

    def is_like_exist(self, userid: str, businessid: str):
        """
        judge whether a like is in the database
        :param userid: user's userid
        :type userid: str
        :param businessid: a service's businessid
        :type businessid: str
        :return: is like exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from likes where userid=%s and businessid=%s;",
                                           [userid, businessid])
        return len(res)

    def is_dislike_exist(self, userid: str, businessid: str):
        """
        judge whether a dislike is in the database
        :param userid: user's userid
        :type userid: str
        :param businessid: a service's businessid
        :type businessid: str
        :return: is dislike exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from dislike where userid=%s and businessid=%s;",
                                           [userid, businessid])
        return len(res)

    def is_superlike_exist(self, userid: str, businessid: str):
        """
        judge whether a superlike is in the database
        :param userid: user's userid
        :type userid: str
        :param businessid: a service's businessid
        :type businessid: str
        :return: is superlike exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from superlike where userid=%s and businessid=%s;",
                                           [userid, businessid])
        return len(res)

    def is_wantGo_exist(self, userid: str, businessid: str):
        """
        judge whether a want go is in the database
        :param userid: user's userid
        :type userid: str
        :param businessid: a service's businessid
        :type businessid: str
        :return: is want go exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from wantgo where userid=%s and businessid=%s;",
                                           [userid, businessid])
        return len(res)

    def is_friend_exist(self, firstuserid: str, seconduserid: str):
        """
        judge whether a friend is in the database
        :param firstuserid: first user's userid who is the owner of the friendship
        :type firstuserid: str
        :param seconduserid: the second user's userid who is the friend of the first user to be checked
        :type seconduserid: str
        :return: is friend exist or not
        :rtype: int
        """
        friends = self.get_friends(userid=firstuserid)
        return seconduserid in friends

    def is_chat_record_exist(self, chatid: str):
        """
        judge whether a chat record is in the database
        :param chatid: a chat's chatid
        :type chatid: str
        :return: is chat record exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from userchat where chatid=%s;",
                                           [chatid])
        return len(res)

    def is_chat_record_recept(self, chatid: str):
        """
        judge whether a chat record has been sent to its receiver
        :param chatid: a chat's chatid
        :type chatid: str
        :return: is chat record recept or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from chatreceipt where chatid=%s and isrecept=1;",
                                           [chatid])
        return len(res)

    def is_tip_exist(self, tipid: str):
        """
        judge whether a tip is in the database
        :param tipid: a tip's tipid
        :type tipid: str
        :return: is tip exist or not
        :rtype: int
        """
        res = self.safe_execute_and_return("select * from tip where tipid=%s;", [tipid])
        return len(res)

    """ insert operations """

    def add_tip(self, tipid: str, userid: str, businessid: str, timestamp: int, content: str):
        """
        insert a new tip into the database
        :param tipid: a tip's tipid
        :type tipid: str
        :param userid: the user's userid who post the tip
        :type userid: str
        :param businessid: the service's businessid included in the tip
        :type businessid: str
        :param timestamp: the time when the tip is posted
        :type timestamp: int
        :param content: the content of the tip
        :type content: str
        :return: result of the insertion
        :rtype: any
        """
        # convert timestamp to format
        f_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        self.insert_data('tip',
                         {'tipid': tipid, 'userid': userid, 'businessid': businessid, 'date': f_time, 'text': content})

    def add_friend(self, firstuserid: str, seconduserid: str):
        """
        insert a new friend of a user
        :param firstuserid: first user's userid who is the owner of the friendship
        :type firstuserid: str
        :param seconduserid: second user's userid who is the friend of the first user to be added
        :type seconduserid: str
        :return: result of the insertion
        :rtype: any
        """
        friends = self.get_friends(userid=firstuserid)
        if seconduserid in friends:
            return

        friends.append(seconduserid)
        friends = ', '.join(friends)
        self.delete_friend(firstuserid=firstuserid)
        self.insert_data('friends', {'firstuserid': firstuserid, 'seconduserid': friends})

    def add_reply(self, reviewid: str, userid: str, timestamp: int, content: str):
        """
        insert a new reply of a review
        :param reviewid: the review's reviewid
        :type reviewid: str
        :param userid: the user's userid who post the reply
        :type userid: str
        :param timestamp: the time when the reply is posted
        :type timestamp: int
        :param content: the content of the reply
        :type content: str
        :return: result of the insertion
        :rtype: any
        """
        # convert timestamp to format
        f_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        self.insert_data('reply', {'reviewid': reviewid, 'userid': userid, 'date': f_time, 'text': content})

    def add_account(self, userid: str, account: str, password: str):
        """
        insert a new account
        :param userid: the user's userid
        :type userid: str
        :param account: the user's account
        :type account: str
        :param password: the user's password
        :type password: str
        :return: is account created or not
        :rtype: int
        """
        self.insert_data('account', {'userid': userid, 'account': account, 'password': password})

    def add_user(self, **kwargs):
        """
        insert a new user
        :param kwargs: userid, registertime and other optional information
        :type kwargs: dict
        :return: is user created or not
        :rtype: int
        """
        self.insert_data('\"user\"', kwargs)

    def add_like(self, userid: str, businessid: str):
        """
        insert a new like info
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is like created or not
        :rtype: int
        """
        self.insert_data('likes', {'userid': userid, 'businessid': businessid})

    def add_dislike(self, userid: str, businessid: str):
        """
        insert a new dislike info
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is dislike created or not
        :rtype: int
        """
        self.insert_data('dislike', {'userid': userid, 'businessid': businessid})

    def add_superlike(self, userid: str, businessid: str):
        """
        insert a new superlike info
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is superlike created or not
        :rtype: int
        """
        self.insert_data('superlike', {'userid': userid, 'businessid': businessid})

    def add_review(self, reviewid: str, userid: str, businessid: str, totalScore: float, date: str, text: str):
        """
        insert a new review into the database
        :param reviewid: the review's reviewid
        :type reviewid: str
        :param userid: the user's userid who post the review
        :type userid: str
        :param businessid: the service's businessid included in the review
        :type businessid: str
        :param totalScore: score of the review
        :type totalScore: float
        :param date: the time when the review is posted
        :type date: str
        :param text: the content of the review
        :type text: str
        :return: is review created or not
        :rtype: int
        """
        self.insert_data('review',
                         {'reviewid': reviewid, 'userid': userid, 'businessid': businessid, 'score': totalScore,
                          'date': date, 'text': text,
                          'useful': 0, 'funny': 0, 'cool': 0})

    def add_wantGo(self, userid: str, businessid: str):
        """
        insert a new want go message into the database
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is want go created or not
        :rtype: int
        """
        self.insert_data('wantgo', {'userid': userid, 'businessid': businessid})

    def add_chat_record(self, chatid: str, firstuserid: str, seconduserid: str, isfirsttosecond: int, context: str,
                        contexttype: str, timestamp: int):
        """
        insert a new chat record into the database
        :param chatid: chat's chatid
        :type chatid: str
        :param firstuserid: the user's userid who is the owner of the chat
        :type firstuserid: str
        :param seconduserid: the user's userid who is the friend of the first user to be added
        :type seconduserid: str
        :param isfirsttosecond: is the first user to send the message to the second user
        :type isfirsttosecond: int
        :param context: the content of the chat
        :type context: str
        :param contexttype: the type of the chat
        :type contexttype: str
        :param timestamp: the time when the chat is sent
        :type timestamp: int
        :return: is chat record created or not
        :rtype: int
        """
        f_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        self.insert_data('userchat',
                         {'chatid': chatid, 'firstuserid': firstuserid, 'seconduserid': seconduserid,
                          'isfirsttosecond': isfirsttosecond,
                          'context': context, 'contexttype': contexttype, 'time': f_time})

    def add_chat_reception(self, chatid: str, isrecept: int):
        """
        insert a new chat reception status into the database
        :param chatid: the chat's chatid
        :type chatid: str
        :param isrecept: the reception status of the chat
        :type isrecept: int
        :return: is chat reception created or not
        :rtype: int
        """
        self.insert_data('chatreceipt', {'chatid': chatid, 'isrecept': isrecept})

    def add_user_at(self, userid: str, businessid: str):
        """
        insert a status of "a new user at a service"
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid witch the user is at
        :type businessid: str
        :return: is user at created or not
        :rtype: int
        """
        self.insert_data('userat', {'userid': userid, 'businessid': businessid})

    def add_friends(self, firstuserid: str, seconduserid: str):
        self.insert_data('friends', {'firstuserid': firstuserid, 'seconduserid': seconduserid})

    """ remove operations """

    def remove_friend(self, firstuserid: str, seconduserid: str):
        """
        remove a friend of a user
        :param firstuserid: the user's userid who is the owner of the friendship
        :type firstuserid: str
        :param seconduserid: the user's userid who is the friend of the first user to be removed
        :type seconduserid: str
        :return: is friend removed or not
        :rtype: int
        """
        friends = self.get_friends(userid=firstuserid)
        friends.remove(seconduserid)
        friends = ', '.join(friends)
        self.delete_friend(firstuserid=firstuserid)

        self.insert_data('friends', {'firstuserid': firstuserid, 'seconduserid': friends})

    """ delete operations """

    def clear_recommend_business_record(self, userid: str):
        """
        clear the recommend business record for a user
        :param userid: the user's userid
        :type userid: str
        :return: is recommended business record cleared or not
        :rtype: int
        """
        self.safe_execute_only("delete from recommended where userid=%s;", [userid])

    def delete_user(self, userid: str):
        """
         delete a user by userid
        :param userid: the user's userid is to be deleted
        :type userid: str
        :return:is user deleted or not
        :rtype:int
        """
        self.safe_execute_only("delete from account where userid=%s;", [userid])
        self.safe_execute_only("delete from \"user\" where userid=%s;", [userid])

    # delete a want go message by userid businessid
    def delete_wantGo(self, userid: str, businessid: str):
        """
        delete a want go message by userid and businessid
        :param userid: the user's userid who want to go to the service
        :type userid: str
        :param businessid:
        :type businessid:
        :return:
        :rtype:
        """
        self.safe_execute_only("delete from wantgo where userid=%s and businessid=%s", [userid, businessid])

    def delete_review(self, reviewid: str):
        """
        delete a review by reviewid
        :param reviewid: the review's reviewid
        :type reviewid: str
        :return: is review removed or not
        :rtype: int
        """
        self.safe_execute_and_return("delete from review where reviewid=%s;", [reviewid])

    def delete_like(self, userid: str, businessid: str):
        """
        delete a like by userid and businessid
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is like removed or not
        :rtype: int
        """
        self.safe_execute_only("delete from likes where (userid=%s) and (businessid=%s);", [userid, businessid])

    def delete_dislike(self, userid: str, businessid: str):
        """
        delete a dislike by userid and businessid
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is dislike removed or not
        :rtype: int
        """
        self.safe_execute_only("delete from dislike where (userid=%s) and (businessid=%s);",
                               [userid, businessid])

    def delete_superlike(self, userid: str, businessid: str):
        """
        delete a superlike by userid and businessid
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is superlike removed or not
        :rtype: int
        """
        self.safe_execute_only("delete from superlike where (userid=%s) and (businessid=%s);",
                               [userid, businessid])

    def delete_friend(self, firstuserid: str):
        """
        delete someone's all friends by firstuserid
        :param firstuserid: the user's userid who is the owner of the friendship
        :type firstuserid: str
        :return: is friends removed or not
        :rtype: int
        """
        self.safe_execute_only("delete from friends where (firstuserid=%s);", [firstuserid])

    def delete_chat_receipt(self, chatid: str):
        """
        delete a chat reception by chatid, which means the chat has been received by the receiver
        :param chatid: chat's chatid
        :type chatid: str
        :return: is chat reception removed or not
        :rtype: int
        """
        self.safe_execute_only("delete from chatreceipt where (chatid=%s);", [chatid])

    def delete_user_at(self, userid: str, businessid: str):
        """
        delete a status of "a user is at a service", which means the user has left the service
        :param userid: the user's userid
        :type userid: str
        :param businessid: the service's businessid
        :type businessid: str
        :return: is user at removed or not
        :rtype: int
        """
        self.safe_execute_only("delete from userat where userid=%s and businessid=%s", [userid, businessid])

    """ update operation """

    def set_chat_recept(self, chatid: str):
        """
        set a chat reception by chatid, which means the chat has been received by the receiver
        :param chatid: the chat's chatid
        :type chatid: str
        :return: is chat reception set or not
        :rtype: int
        """
        self.delete_chat_receipt(chatid=chatid)
        self.add_chat_reception(chatid=chatid, isrecept=1)

    def update_username(self, userid: str, newname: str):
        """
        update a user's username by userid
        :param userid: user's userid
        :type userid: str
        :param newname: user's new username
        :type newname: str
        :return: is username updated or not
        :rtype: int
        """
        self.safe_execute_only("update \"user\" set username = %s where userid=%s", [newname, userid])


def main():
    handler = ProjectSQLProxy(host='192.168.0.120', user='root', password='Ecnu202408HuaWeiYun@',
                              db='project', port=5432, charset='utf8')
    handler.set_auto_commit(True)

    print(handler.get_recommends_business(userid='eAqjr-5h0PpzmBcUmNrLXw'))


if __name__ == '__main__':
    main()
