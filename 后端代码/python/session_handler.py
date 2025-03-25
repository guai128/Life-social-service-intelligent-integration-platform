from typing import List
from sqlProxy import ProjectSQLProxy
import json
import random
import time

size = len('___hQj63mwgFDhehNwv3ZQ')


class SessionHandler:
    def __init__(self, host, user, password, db, port, charset='utf8'):
        self.sqlProxy = ProjectSQLProxy(host, user, password, db, port, charset)
        self.types = ['login', 'register', 'logout', 'get_services', 'user_swipe', 'get_reviews', 'get_user_reviews',
                      'review', 'get_superlike', 'remove_superlike', 'heartbeat', 'get_service_info', 'garbage',
                      'get_tips', 'get_wantgo', 'wantgo', 'add_chat_record', 'transmit_message',
                      'request_relax_chat_id', 'pull_no_receipt_chat', 'acknowledge_chat_receipt', 'cancel_wantgo',
                      'add_like', 'add_superlike', 'add_dislike', 'cancel_like', 'cancel_superlike', 'cancel_dislike',
                      'is_wantgo_exist', 'is_like_exist', 'is_superlike_exist', 'is_dislike_exist', 'get_user_info',
                      'register_user_at', 'remove_user_at', 'get_review_replies', 'get_post_reviews',
                      'get_relative_posts', 'reply', 'follow', 'get_follows', 'get_fans', 'update_profile',
                      'pre_upload_file', 'get_user_post', 'get_user_superlike', 'get_user_like', 'is_follow_exist',
                      'is_fan_exist', 'cancel_follow', 'post', 'get_post_info', 'get_fans_num', 'get_follow_num']

        self.funcs = {self.types[0]: self.loginHandle, self.types[1]: self.registerHandle,
                      self.types[2]: self.logoutHandle, self.types[3]: self.getServicesHandle,
                      self.types[4]: self.userSwipeHandle, self.types[5]: self.getReviewsHandle,
                      self.types[6]: self.getUsersReviewsHandle, self.types[7]: self.reviewHandle,
                      self.types[8]: self.getSuperlikeHandle, self.types[9]: self.removeSuperlikeHandle,
                      self.types[10]: self.heartBeat, self.types[11]: self.getServiceInfoHandle,
                      self.types[12]: self.garbageHandle, self.types[13]: self.getTipsHandle,
                      self.types[14]: self.getWantGoHandle, self.types[15]: self.wantGoHandle,
                      self.types[16]: self.addChatRecordHandle, self.types[17]: self.transmitMessageHandle,
                      self.types[18]: self.requestRelaxChatIdHandle, self.types[19]: self.pullNoReceiptChatHandle,
                      self.types[20]: self.acknowledgeChatReceiptHandle, self.types[21]: self.cancelWantGoHandle,
                      self.types[22]: self.addLikeHandle, self.types[23]: self.addSuperlikeHandle,
                      self.types[24]: self.addDislikeHandle, self.types[25]: self.cancelLikeHandle,
                      self.types[26]: self.cancelSuperlikeHandle, self.types[27]: self.cancelDislikeHandle,
                      self.types[28]: self.isWantGoExistHandle, self.types[29]: self.isLikeExistHandle,
                      self.types[30]: self.isSuperlikeExistHandle, self.types[31]: self.isDislikeExistHandle,
                      self.types[32]: self.getUserInfoHandle, self.types[33]: self.registerUserAtServiceHandle,
                      self.types[34]: self.removeUserAtServiceHandle, self.types[35]: self.getReviewRepliesHandle,
                      self.types[36]: self.getPostReviewsHandle, self.types[37]: self.getRelativePostsHandle,
                      self.types[38]: self.replyHandle, self.types[39]: self.followHandle,
                      self.types[40]: self.getFollowsHandle, self.types[41]: self.getFansHandle,
                      self.types[42]: self.updateProfileHandle, self.types[43]: self.preUploadFileHandle,
                      self.types[44]: self.getUserPostHandle, self.types[45]: self.getUserSuperlikeHandle,
                      self.types[46]: self.getUserLikeHandle, self.types[47]: self.isFollowExistHandle,
                      self.types[48]: self.isFanExistHandle, self.types[49]: self.cancelFollowHandle,
                      self.types[50]: self.PostHandle, self.types[51]: self.getPostInfoHandle,
                      self.types[52]: self.getFansNumHandle, self.types[53]: self.getFollowNumHandle}

        self.dependencies = {self.types[0]: ['account', 'password'],
                             self.types[1]: ['username', 'password'],
                             self.types[2]: ['account'],
                             self.types[3]: ['userid'],
                             self.types[4]: ['userid', 'businessid', 'swipetype'],
                             self.types[5]: ['businessid', 'start'],
                             self.types[6]: ['userid', 'start'],
                             self.types[7]: ['userid', 'businessid', 'score', 'text'],
                             self.types[8]: ['userid'],
                             self.types[9]: ['userid', 'businessid'],
                             self.types[10]: [],
                             self.types[11]: ['businessid'],
                             self.types[12]: [],
                             self.types[13]: ['userid'],
                             self.types[14]: ['businessid'],
                             self.types[15]: ['userid', 'businessid'],
                             self.types[16]: ['firstuserid', 'seconduserid', 'isfirsttosecond', 'context',
                                              'contexttype', 'time'],
                             self.types[17]: ['firstuserid', 'seconduserid', 'isfirsttosecond', 'context',
                                              'contexttype', 'time'],
                             self.types[18]: [],
                             self.types[19]: ['userid'],
                             self.types[20]: ['chatid'],
                             self.types[21]: ['userid', 'businessid'],
                             self.types[22]: ['userid', 'businessid'],
                             self.types[23]: ['userid', 'businessid'],
                             self.types[24]: ['userid', 'businessid'],
                             self.types[25]: ['userid', 'businessid'],
                             self.types[26]: ['userid', 'businessid'],
                             self.types[27]: ['userid', 'businessid'],
                             self.types[28]: ['userid', 'businessid'],
                             self.types[29]: ['userid', 'businessid'],
                             self.types[30]: ['userid', 'businessid'],
                             self.types[31]: ['userid', 'businessid'],
                             self.types[32]: ['userid'],
                             self.types[33]: ['userid', 'businessid'],
                             self.types[34]: ['userid', 'businessid'],
                             self.types[35]: ['businessid', 'start'],
                             self.types[36]: ['tipid', 'start'],
                             self.types[37]: ['businessid', 'start'],
                             self.types[38]: ['reviewid', 'userid', 'content', 'time'],
                             self.types[39]: ['firstuserid', 'seconduserid'],
                             self.types[40]: ['userid'],
                             self.types[41]: ['userid'],
                             self.types[42]: ['userid', 'name'],
                             self.types[43]: ['file_type'],
                             self.types[44]: ['userid'],
                             self.types[45]: ['userid', 'start'],
                             self.types[46]: ['userid', 'start'],
                             self.types[47]: ['firstuserid', 'seconduserid'],
                             self.types[48]: ['firstuserid', 'seconduserid'],
                             self.types[49]: ['firstuserid', 'seconduserid'],
                             self.types[50]: ['userid', 'businessid', 'text', 'date'],
                             self.types[51]: ['tipid'],
                             self.types[52]: ['userid'],
                             self.types[53]: ['userid']}

    def __enter__(self):
        return self

    def handle(self, request_text):
        """
        handle the request text and execute the corresponding function according to the type in request_json
        :param request_text: request text accepted by the server
        :type request_text: str
        :return: handle result in json format
        :rtype: dict
        """
        try:
            request_json = json.loads(request_text)
        except json.JSONDecodeError:
            res_json = {'status': 'fail', 'message': 'json decode error'}
            return res_json

        if 'type' not in request_json:
            res_json = {'status': 'fail', 'message': 'type not in request'}
            return res_json

        if request_json['type'] not in self.types:
            res_json = {'status': 'fail', 'message': 'type not in types'}
            if 'task_id' in request_json:
                res_json['task_id'] = request_json['task_id']
            return res_json

        is_meet, tip = self.request_json_check(request_json, self.dependencies[request_json['type']])
        if not is_meet:
            return {'status': 'fail', 'message': tip}

        ret = self.funcs[request_json['type']](request_json)
        if len(ret) == 2:
            status, message = ret
            data = None
        elif len(ret) == 3:
            status, message, data = ret
        else:
            status, message = 'fail', 'unknown error'
            data = None

        res_json = {'status': status, 'message': message}
        if data is not None:
            res_json['data'] = data
        if 'task_id' in request_json:
            res_json['task_id'] = request_json['task_id']

        return res_json

    @staticmethod
    def request_json_check(request_json, requires: List[str]):
        """
        check whether the requirements are all in request_json's keys
        :param request_json: request json dict is to be checked
        :type request_json: dict
        :param requires: the requirements
        :type requires: list
        :return:
        :rtype:
        """
        un_meet_requires = [re for re in requires if re not in request_json]
        return len(un_meet_requires) == 0, ", ".join(un_meet_requires) + ' not in request_json'

    def loginHandle(self, request_json):
        account = request_json['account']
        password = request_json['password']

        # search the account
        if not self.sqlProxy.is_account_exist(account):
            return 'fail', 'no this account'

        # check the password
        if not self.sqlProxy.is_login_success(account, password):
            return 'fail', 'account or password error'

        return 'success', 'login success', {'userid': self.sqlProxy.get_userid_by_account_number(account)}

    def registerHandle(self, request_json):
        username = request_json['username']
        password = request_json['password']

        # random generate the account number (Ex: 12345678910)
        random_account = None
        while random_account is None or self.sqlProxy.is_account_exist(random_account):
            random_account = ''.join(
                random.choices('0123456789', k=11))

        # random generate the id (Ex: ___hQj63mwgFDhehNwv3ZQ)
        random_id = None
        while random_id is None or self.sqlProxy.is_userid_exist(random_id):
            random_id = ''.join(
                random.choices('_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                               k=size))

        register_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        self.sqlProxy.add_user(userid=random_id, username=username, registertime=register_date)
        self.sqlProxy.add_account(userid=random_id, account=random_account, password=password)
        self.sqlProxy.commit()

        return 'success', 'register success', {'userid': random_id, 'account': random_account}

    def logoutHandle(self, request_json):
        account = request_json['account']
        if not self.sqlProxy.is_account_exist(account):
            return 'fail', 'no this account'

        userid = self.sqlProxy.get_userid_by_account_number(account)
        self.sqlProxy.delete_user(userid=userid)
        self.sqlProxy.commit()

        return 'success', 'logout success'

    def getServicesHandle(self, request_json):
        userid = request_json['userid']
        if not self.sqlProxy.is_userid_exist(userid):
            return 'fail', 'no this user'

        if 'count' not in request_json:
            count = 1
        else:
            count = request_json['count']

        # random select count service
        businesses = self.sqlProxy.get_recommends_business(userid=userid, count=count)
        if len(businesses) == 0:
            return 'fail', 'no service'
        ret_data = []
        for row in businesses:
            temp_data = self.sqlProxy.get_service_info(row[0])
            # get picture
            picInfo = self.sqlProxy.get_picture(businessid=temp_data['businessid'])
            temp_data['image'] = []
            if picInfo is not None:
                for info in picInfo:
                    temp_data['image'].append(
                        f"http://124.70.185.215:1414/servicePictures/{info[0]}.jpg")  # http://124.70.185.215:1414/
            ret_data.append(temp_data)

        return 'success', 'get services success', ret_data

    def userSwipeHandle(self, request_json):
        userid = request_json['userid']
        if not self.sqlProxy.is_userid_exist(userid):
            return 'fail', 'no this account'

        # get the business id
        businessid = request_json['businessid']
        if businessid is None:
            return 'fail', 'no this restaurant'

        if request_json['swipetype'] == 'like':
            self.sqlProxy.delete_dislike(userid, businessid)
            self.sqlProxy.delete_superlike(userid, businessid)
            if not self.sqlProxy.is_like_exist(userid=userid, businessid=businessid):
                self.sqlProxy.add_like(userid, businessid)
        elif request_json['swipetype'] == 'dislike':
            self.sqlProxy.delete_like(userid, businessid)
            self.sqlProxy.delete_superlike(userid, businessid)
            if not self.sqlProxy.is_dislike_exist(userid=userid, businessid=businessid):
                self.sqlProxy.add_dislike(userid, businessid)
        elif request_json['swipetype'] == 'superlike':
            self.sqlProxy.delete_like(userid, businessid)
            self.sqlProxy.delete_dislike(userid, businessid)
            if not self.sqlProxy.is_superlike_exist(userid=userid, businessid=businessid):
                self.sqlProxy.add_superlike(userid, businessid)
        elif request_json['swipetype'] == 'skip':
            pass
        else:
            return 'fail', 'swipetype error'

        self.sqlProxy.commit()

        return 'success', 'swipe success'

    def getReviewsHandle(self, request_json):
        businessid = request_json['businessid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        ret_data = self.sqlProxy.get_reviews_by_businessid(businessid, start)

        return 'success', 'get reviews success', ret_data

    def getUsersReviewsHandle(self, request_json):
        userid = request_json['userid']
        if not self.sqlProxy.is_userid_exist(userid):
            return 'fail', 'no this user'
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        ret_data = self.sqlProxy.get_reviews_by_userid(userid, start)
        if len(ret_data) == 0:
            return 'fail', 'no reviews'

        return 'success', 'get user reviews success', ret_data

    def reviewHandle(self, request_json):
        userid = request_json['userid']
        if not self.sqlProxy.is_userid_exist(userid):
            return 'fail', 'no this account'

        businessid = request_json['businessid']
        score = request_json['score']
        text = request_json['text']
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        reviewid = None
        while reviewid is None or self.sqlProxy.is_review_exist(reviewid):
            reviewid = ''.join(
                random.choices('_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                               k=size))

        self.sqlProxy.add_review(reviewid, userid, businessid, score, date, text)
        self.sqlProxy.commit()

        return 'success', 'review success'

    def getSuperlikeHandle(self, request_json):
        userid = request_json['userid']
        ret_data = self.sqlProxy.get_all_user_superlike(userid=userid)

        return 'success', 'get superlike success', ret_data

    def removeSuperlikeHandle(self, request_json):
        userid = request_json['userid']
        if not self.sqlProxy.is_userid_exist(userid):
            return 'fail', 'no this user'

        businessid = request_json['businessid']
        self.sqlProxy.delete_superlike(userid, businessid)
        self.sqlProxy.commit()
        return 'success', 'remove superlike success'

    def getServiceInfoHandle(self, request_json):
        businessid = request_json['businessid']
        if not self.sqlProxy.is_business_exist(businessid):
            return 'fail', 'no this business'

        data = self.sqlProxy.get_service_info(businessid)
        picInfo = self.sqlProxy.get_picture(businessid=businessid)
        data['image'] = []
        if picInfo is not None:
            for info in picInfo:
                data['image'].append(f"http://124.70.185.215:1414/servicePictures/{info[0]}.jpg")

        return 'success', 'get service info success', data

    def heartBeat(self, request_json):
        print(request_json)
        try:
            self.sqlProxy.safe_execute_only('select 1')
        except Exception:
            return 'fail', 'heartbeat fail'
        return 'success', 'heartbeat success'

    @staticmethod
    def garbageHandle(request_json):
        print(request_json)
        return 'no reply', 'this is a garbage'

    def getTipsHandle(self, request_json):
        userid = request_json['userid']

        if 'count' not in request_json:
            count = 1
        else:
            count = request_json['count']

        # random select count service
        tips = self.sqlProxy.get_recommends_tips(userid=userid, count=count)
        if len(tips) == 0:
            return 'fail', 'no service'

        ret_data = []
        for row in tips:
            ret_data.append(self.sqlProxy.get_post_info(tipid=row[0]))

        return 'success', 'get tips success', ret_data

    def getWantGoHandle(self, request_json):
        businessid = request_json['businessid']

        wantGos = self.sqlProxy.get_wantGos_by_businessid(businessid=businessid)
        ret_data = []
        for user in wantGos:
            info = self.sqlProxy.get_userInfo(user)
            if info is not None:
                ret_data.append(info)

        return 'success', 'get want gos success', ret_data

    def wantGoHandle(self, request_json):
        userid = request_json['userid']
        businessid = request_json['businessid']
        self.sqlProxy.add_wantGo(userid=userid, businessid=businessid)
        self.sqlProxy.commit()

        return 'success', 'add want gos success'

    def addChatRecordHandle(self, request_json):
        chatid = request_json['chatid']
        while chatid is None or self.sqlProxy.is_chat_record_exist(chatid):
            chatid = ''.join(
                random.choices('_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=size))

        firstuserid = request_json['firstuserid']
        seconduserid = request_json['seconduserid']
        isfirsttosecond = request_json['isfirsttosecond']
        context = request_json['context']
        contexttype = request_json['contexttype']
        timestamp = request_json['time']

        self.sqlProxy.add_chat_record(chatid, firstuserid, seconduserid, isfirsttosecond, context, contexttype, timestamp)
        self.sqlProxy.commit()

        return 'success', 'add want gos success'

    def transmitMessageHandle(self, request_json):
        chatid = None
        while chatid is None or self.sqlProxy.is_chat_record_exist(chatid):
            chatid = ''.join(
                random.choices('_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=size))
        request_json['chatid'] = chatid
        # register in database
        self.addChatRecordHandle(request_json)
        self.sqlProxy.add_chat_reception(request_json['chatid'], 0)
        self.sqlProxy.commit()

        return 'success', 'can transmit', request_json

    def requestRelaxChatIdHandle(self, request_json):
        print(request_json)
        chatid = None
        while chatid is None or self.sqlProxy.is_chat_record_exist(chatid):
            chatid = ''.join(
                random.choices('_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=size))

        return 'success', 'get relax chatid success', {'chatid': chatid}

    def pullNoReceiptChatHandle(self, request_json):
        chatRecords = self.sqlProxy.get_unreceptive_chat(userid=request_json['userid'])
        return 'success', 'get under recept chat record success', chatRecords

    def acknowledgeChatReceiptHandle(self, request_json):
        if not self.sqlProxy.is_chat_record_exist(request_json['chatid']):
            return 'fail', 'no this chatid'

        self.sqlProxy.set_chat_recept(request_json['chatid'])
        self.sqlProxy.commit()
        return 'success', 'acknowledge success'

    def cancelWantGoHandle(self, request_json):
        self.sqlProxy.delete_wantGo(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'cancel want go success'

    def addLikeHandle(self, request_json):
        self.sqlProxy.add_like(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.delete_dislike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'add like success'

    def addSuperlikeHandle(self, request_json):
        self.sqlProxy.add_superlike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.delete_dislike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'add superlike success'

    def addDislikeHandle(self, request_json):
        self.sqlProxy.add_dislike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.delete_like(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.delete_superlike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'add dislike success'

    def cancelLikeHandle(self, request_json):
        self.sqlProxy.delete_like(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'cancel like success'

    def cancelSuperlikeHandle(self, request_json):
        self.sqlProxy.delete_superlike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'cancel superlike success'

    def cancelDislikeHandle(self, request_json):
        self.sqlProxy.delete_dislike(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'cancel dislike success'

    def isWantGoExistHandle(self, request_json):
        exist = self.sqlProxy.is_wantGo_exist(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'query weather want go exist success', {'exist': exist}

    def isLikeExistHandle(self, request_json):
        exist = self.sqlProxy.is_like_exist(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'query weather like exist success', {'exist': exist}

    def isSuperlikeExistHandle(self, request_json):
        exist = self.sqlProxy.is_superlike_exist(userid=request_json['userid'], businessid=request_json['businessid'])

        return 'success', 'query weather superlike exist success', {'exist': exist}

    def isDislikeExistHandle(self, request_json):
        exist = self.sqlProxy.is_dislike_exist(userid=request_json['userid'], businessid=request_json['businessid'])

        return 'success', 'query weather superlike exist success', {'exist': exist}

    def getUserInfoHandle(self, request_json):
        ret_data = self.sqlProxy.get_userInfo(userid=request_json['userid'])
        if ret_data is None:
            return 'fail', 'no this user'

        return 'success', 'query weather superlike exist success', ret_data

    def registerUserAtServiceHandle(self, request_json):
        self.sqlProxy.add_user_at(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'register user at service success'

    def removeUserAtServiceHandle(self, request_json):
        self.sqlProxy.delete_user_at(userid=request_json['userid'], businessid=request_json['businessid'])
        self.sqlProxy.commit()

        return 'success', 'cancel user at service success'

    def getReviewRepliesHandle(self, request_json):
        reviewid = request_json['reviewid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        ret_data = self.sqlProxy.get_reviews_by_businessid(reviewid, start)

        return 'success', 'get reviews success', ret_data

    def getPostReviewsHandle(self, request_json):
        tipid = request_json['tipid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'
        businessid = self.sqlProxy.get_service_from_tip(tipid=tipid)
        if businessid is None:
            return 'fail', 'no this post or this post don\'t refer to some service'

        ret_data = self.sqlProxy.get_reviews_by_businessid(businessid, start)

        return 'success', 'get reviews success', ret_data

    def getRelativePostsHandle(self, request_json):
        businessid = request_json['businessid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        ret_data = []
        rows = self.sqlProxy.get_tips_by_businessid(businessid, start)
        for row in rows:
            ret_data.append(self.sqlProxy.get_post_info(tipid=row))

        return 'success', 'get posts success', ret_data

    def replyHandle(self, request_json):
        userid = request_json['userid']
        reviewid = request_json['reviewid']
        timestamp = request_json['time']
        content = request_json['content']

        self.sqlProxy.add_reply(reviewid=reviewid, userid=userid, timestamp=timestamp, content=content)
        self.sqlProxy.commit()

        return 'success', 'reply success'

    def followHandle(self, request_json):
        firstuserid = request_json['firstuserid']
        seconduserid = request_json['seconduserid']

        self.sqlProxy.add_friend(firstuserid=firstuserid, seconduserid=seconduserid)
        self.sqlProxy.commit()

        return 'success', 'add follows success'

    def getFollowsHandle(self, request_json):
        userid = request_json['userid']
        follows = self.sqlProxy.get_friends(userid=userid)
        ret_data = []
        for userid in follows:
            info = self.sqlProxy.get_userInfo(userid=userid)
            if info is not None:
                ret_data.append(info)

        return 'success', 'get follows success', ret_data

    def getFansHandle(self, request_json):
        userid = request_json['userid']
        fans = self.sqlProxy.get_friends(userid=userid)
        ret_data = []
        for userid in fans:
            info = self.sqlProxy.get_userInfo(userid=userid)
            if info is not None:
                ret_data.append(info)

        return 'success', 'get fans success', ret_data

    def updateProfileHandle(self, request_json):
        userid = request_json['userid']
        name = request_json['name']

        self.sqlProxy.update_username(userid=userid, newname=name)
        self.sqlProxy.commit()

        return 'success', 'update profile success'

    def preUploadFileHandle(self, request_json):
        pass

    def getUserPostHandle(self, request_json):
        userid = request_json['userid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        tips = self.sqlProxy.get_posts_by_userid(userid, start)
        ret_data = []
        for row in tips:
            ret_data.append(self.sqlProxy.get_post_info(tipid=row[0]))

        return 'success', 'get posts success', ret_data

    def getUserSuperlikeHandle(self, request_json):
        userid = request_json['userid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        ret_data = self.sqlProxy.get_user_superlike(userid=userid, start=start)
        return 'success', 'get superlike success', ret_data

    def getUserLikeHandle(self, request_json):
        userid = request_json['userid']
        start = request_json['start']
        if start < 0:
            return 'fail', 'start must be greater than 0'

        ret_data = self.sqlProxy.get_user_like(userid=userid, start=start)
        return 'success', 'get superlike success', ret_data

    def isFollowExistHandle(self, request_json):
        firstuserid = request_json['firstuserid']
        seconduserid = request_json['seconduserid']

        exist = self.sqlProxy.is_friend_exist(firstuserid=firstuserid, seconduserid=seconduserid)
        return 'success', 'query weather follow exist success', {'exist': exist}

    def isFanExistHandle(self, request_json):
        firstuserid = request_json['firstuserid']
        seconduserid = request_json['seconduserid']
        exist = self.sqlProxy.is_friend_exist(firstuserid=seconduserid, seconduserid=firstuserid)
        return 'success', 'query weather fan exist success', {'exist': exist}

    def cancelFollowHandle(self, request_json):
        firstuserid = request_json['firstuserid']
        seconduserid = request_json['seconduserid']

        self.sqlProxy.remove_friend(firstuserid=seconduserid, seconduserid=firstuserid)
        self.sqlProxy.commit()

        return 'success', 'cancel follow success'

    def PostHandle(self, request_json):
        userid = request_json['userid']
        businessid = request_json['businessid']
        text = request_json['text']
        date = request_json['date']

        tipid = None
        while tipid is None or self.sqlProxy.is_tip_exist(tipid=tipid):
            tipid = ''.join(
                random.choices('_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=size))

        self.sqlProxy.add_tip(tipid=tipid, userid=userid, businessid=businessid, timestamp=date, content=text)
        self.sqlProxy.commit()

        return 'success', 'post successfully', {'tipid': tipid}

    def getPostInfoHandle(self, request_json):
        tipid = request_json['tipid']
        ret_data = self.sqlProxy.get_post_info(tipid=tipid)

        return 'success', 'get post info successfully', ret_data

    def getFansNumHandle(self, request_json):
        userid = request_json['userid']
        ret_data = {'count': len(self.sqlProxy.get_friends(userid=userid))}
        return 'success', 'get fans count successfully', ret_data

    def getFollowNumHandle(self, request_json):
        userid = request_json['userid']
        ret_data = {'count': len(self.sqlProxy.get_friends(userid=userid))}
        return 'success', 'get follows count successfully', ret_data


def main():
    handler = SessionHandler(host='192.168.0.120', user='root', password='Ecnu202408HuaWeiYun@',
                             db='project', port=5432, charset='utf8')
    print(handler.registerHandle({'username': 'wb', 'password': 'wb'}))


if __name__ == '__main__':
    main()
