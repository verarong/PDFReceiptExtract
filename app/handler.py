import pdfplumber
import requests
import json
from app.config import CALLBACK_URL, ufile_bucket_Name, ufile_public_key, ufile_private_key, region, ufile_url
from app.key_dicts import row_keys, column_keys, splits, y_concat_threshold, common, right_threshold, button_threshold, \
    multiple_lines_info, duplicate_keys
from app.utils import debug, logger
import requests as req
from io import BytesIO
import re
import time
from ufile import filemanager

putufile_handler = filemanager.FileManager(ufile_public_key, ufile_private_key)
# row_keys按len排序，优先match长key
row_keys = {k: sorted(v, key=lambda x: len(x), reverse=True) for k, v in row_keys.items()}


class TextBox:
    def __init__(self, text, x0, top, x1, bottom, upright, bank_type):
        self.text = text
        self.upright = upright
        self.x1 = float(x0)
        self.y1 = float(top)
        self.x2 = float(x1)
        self.y2 = float(bottom)
        self.box = (x0, top, x1, bottom)
        self.width = abs(x0 - x1)
        self.height = abs(top - bottom)
        self.center = self.x, self.y = self._get_center()
        self.bank_type = bank_type
        self.extract_flag = False

    def summary(self):
        print(self.text, self.x1, self.y1, self.x2, self.y2)

    def _get_center(self):
        return (self.x1 + self.x2) / 2, (self.y1 + self.y2) / 2

    ####################################################################################################################
    #                                                   |          |
    #                       1                           |     2    |                        3
    #                                                   |          |
    # -----------------------------------------------------------------------------------------------------------------#
    #                       4                           |  self(5) |                        6
    # -----------------------------------------------------------------------------------------------------------------#
    #                                                   |          |
    #                       7                           |     8    |                        9
    #                                                   |          |
    ####################################################################################################################

    def get_direction(self, box_):
        if box_.x > self.x2:
            x_score = 3
        elif box_.x < self.x1:
            x_score = 1
        else:
            x_score = 2
        if box_.y > self.y2:
            y_score = 2
        elif box_.y < self.y1:
            y_score = 0
        else:
            y_score = 1
        return x_score + y_score * 3

    def filter_by_direction(self, boxes, direction):
        return [box for box in boxes if self.get_direction(box) == direction]

    def check_self(self, box):
        if self.get_direction(box) == 5:
            return True
        return False

    def check_right(self, box, x_loose_ratio=1):
        if self.get_direction(box) == 6 and box.x1 - self.x2 < right_threshold.get(self.bank_type, 40) * x_loose_ratio:
            return True
        return False

    def check_right_by_loose_param(self, box, x_loose_ratio):
        return box.x1 - self.x2 < right_threshold.get(self.bank_type, 40) * x_loose_ratio

    def check_above_by_loose_param(self, box, y_loose_ratio):
        return self.y1 - box.y2 < button_threshold * y_loose_ratio

    def check_button_by_loose_param(self, box, y_loose_ratio):
        return box.y1 - self.y2 < button_threshold * y_loose_ratio

    def find_right(self, boxes, x_loose_ratio=1, sort_by_y=True):
        legal = []
        for box in boxes:
            if self.get_direction(box) == 6 and self.check_right_by_loose_param(box, x_loose_ratio):
                legal.append(box)
        legal = sorted(legal, key=lambda j: (j.y, j.x)) if sort_by_y else sorted(legal, key=lambda j: (j.x, j.y))
        return legal

    def find_right_above(self, boxes, x_loose_ratio=1, y_loose_ratio=1, sort_by_y=True):
        legal = []
        for box in boxes:
            if self.get_direction(box) == 3 and self.check_right_by_loose_param(box, x_loose_ratio) and \
                    self.check_above_by_loose_param(box, y_loose_ratio):
                legal.append(box)
        legal = sorted(legal, key=lambda j: (j.y, j.x)) if sort_by_y else sorted(legal, key=lambda j: (j.x, j.y))
        return legal

    def find_right_button(self, boxes, x_loose_ratio=1, y_loose_ratio=1, sort_by_y=True):
        legal = []
        for box in boxes:
            if self.get_direction(box) == 9 and self.check_right_by_loose_param(box, x_loose_ratio) and \
                    self.check_button_by_loose_param(box, y_loose_ratio):
                legal.append(box)
        legal = sorted(legal, key=lambda j: (j.y, j.x)) if sort_by_y else sorted(legal, key=lambda j: (j.x, j.y))
        return legal

    def find_button(self, boxes, y_loose_ratio=1, sort_by_y=True):
        legal = []
        for box in boxes:
            if self.get_direction(box) == 8 and self.check_button_by_loose_param(box, y_loose_ratio):
                legal.append(box)
        legal = sorted(legal, key=lambda j: (j.y, j.x)) if sort_by_y else sorted(legal, key=lambda j: (j.x, j.y))
        return legal

    def find_line_break(self, boxes, y_loose_ratio=1):
        legal = [box for box in boxes if
                 self.get_direction(box) == 7 and self.check_button_by_loose_param(box, y_loose_ratio)]
        legal = sorted(legal, key=lambda box: box.x)
        return legal[:1]

    def find_by_direction(self, boxes, direction):
        if direction == 8:
            return self.find_button(boxes)
        if direction == 3:
            return self.find_right_above(boxes)
        if direction == 6:
            return self.find_right(boxes)
        if direction == "6+":
            return self.find_right(boxes, 10)
        if direction == 9:
            return self.find_right_button(boxes)
        if direction == "line_break":
            return self.find_line_break(boxes)
        return []

    def can_extract(self):
        return not self.extract_flag


class Receipt:
    def __init__(self, bank_type, page_id, receipt_id, text_boxes):
        self.bank_type = bank_type
        self.text_boxes = text_boxes
        self.extract_info = {"page_id": page_id, "receipt_id": receipt_id}
        self._update_extract_info(self._get_range())

    def _update_extract_info(self, info):
        for k, v in info.items():
            if k in self.extract_info:
                if isinstance(self.extract_info[k], list):
                    if v and v not in self.extract_info[k]:
                        self.extract_info[k].append(v)
                elif self.extract_info[k]:
                    if v and self.extract_info[k] != v:
                        self.extract_info[k] = [self.extract_info[k], v]
                else:
                    self.extract_info[k] = v
            else:
                self.extract_info[k] = v

    def _get_range(self):
        y = []
        for box in self.text_boxes:
            y.append(box.y1)
            y.append(box.y2)
        return {"y1": min(y), "y2": max(y)}

    def _check_multiple_lines(self, anchor):
        return anchor in multiple_lines_info[self.bank_type]

    def _get_legal_multiple_lines(self, anchor_box, text, pre_extracted):
        direction = multiple_lines_info[self.bank_type][text]
        if not isinstance(direction, tuple):
            direction = [direction]
        info = ""
        extracted = []
        legals = []
        for x in direction:
            legals += anchor_box.find_by_direction(self.text_boxes, x)
        for legal in legals:
            if legal and legal not in extracted + pre_extracted and not re.findall("|".join(row_keys[self.bank_type]),
                                                                                   legal.text):
                extracted.append(legal)
                legal.extract_flag = True
                info += legal.text
            # print(info,legal.text)
        return info

    def _extract_by_colon(self):
        for box in self.text_boxes:
            if re.findall(r'[^0123456789][:：]', box.text):
                start, end = re.search(r'[:：]', box.text).span()
                box.extract_flag = True
                extend = ""
                if self._check_multiple_lines(box.text[:start]):
                    extend = self._get_legal_multiple_lines(box, box.text[:start], [])
                info = {box.text[:start]: box.text[end:] + extend}
                self._update_extract_info_with_check_duplicate_keys(box.text[:start], info)

    @staticmethod
    def concat(boxes, sort_by_x=True):
        if sort_by_x:
            boxes = sorted(boxes, key=lambda j: (j.x, j.y))
        else:
            boxes = sorted(boxes, key=lambda j: (j.y, j.x))
        concat = ""
        index_to_box = {}
        start = 0
        for box in boxes:
            for i, char in enumerate(box.text):
                concat += char
                index_to_box[start + i] = box
            start += len(box.text)
        return concat, index_to_box

    def _concat_row(self, boxes=None, exclude_dict=False):
        if boxes:
            sorted_boxes = sorted(boxes, key=lambda j: (j.y, j.x))
        else:
            sorted_boxes = sorted(self.text_boxes, key=lambda j: (j.y, j.x))
        rows = []
        temp = []
        y = 0
        for box in sorted_boxes:
            if temp and abs(box.y - y) > y_concat_threshold.get(self.bank_type, 10):
                rows.append(self.concat(temp))
                temp = [box]
                y = box.y
            elif temp:
                temp.append(box)
            else:
                temp = [box]
                y = box.y
        if temp:
            rows.append(self.concat(temp))
        if exclude_dict:
            return [x for x, _ in rows]
        return rows

    def _zip_by_columns(self, boxes):
        sorted_boxes = sorted(boxes, key=lambda j: (j.y, j.x))
        rows = []
        temp = []
        y = 0
        for box in sorted_boxes:
            if temp and abs(box.y - y) > y_concat_threshold.get(self.bank_type, 10):
                rows.append(temp)
                temp = [box.text]
                y = box.y
            elif temp:
                temp.append(box.text)
            else:
                temp = [box.text]
                y = box.y
        if temp:
            rows.append(temp)
        columns = list(zip(*rows))
        return columns

    def _update_extract_info_with_check_duplicate_keys(self, re_match, info):
        if self.bank_type in duplicate_keys:
            keys, info_expand, info_expand_ = duplicate_keys[self.bank_type]
            if re_match in keys and re_match in self.extract_info:
                info_ = {info_expand + re_match: self.extract_info[re_match],
                         info_expand_ + re_match: info[re_match]}
                self._update_extract_info(info_)
                del self.extract_info[re_match]
            else:
                self._update_extract_info(info)
        else:
            self._update_extract_info(info)

    def _match_row_keys(self, anchor_boxes, indexes, re_matches, index_to_box):
        for anchor, index, re_match in zip(anchor_boxes, indexes, re_matches):
            for i in range(len(re_match)):
                index_to_box[index + i].extract_flag = True
            next_index = len(re_match) + index
            value_index = None
            if next_index in index_to_box:
                if anchor.check_self(index_to_box[next_index]) or anchor.check_right(index_to_box[next_index]):
                    value_index = next_index

            key_ = re.sub("[：:]", "", re_match)
            content = ""
            pre_extracted = []
            if value_index:
                box = index_to_box[value_index]
                box.extract_flag = True
                pre_extracted.append(box)
                content = re.sub(".*[" + "][".join(key_) + "][：:]?", "", box.text)
            if self._check_multiple_lines(key_):
                content += self._get_legal_multiple_lines(anchor, key_, pre_extracted)
            info = {key_: content}
            self._update_extract_info_with_check_duplicate_keys(key_, info)

    def _extract_by_row_keys(self, rows):
        keys = [k for k in row_keys[self.bank_type] if k not in self.extract_info or not self.extract_info[k]]
        re_char = "[:：]?|".join(keys) + '[:：]?'
        if re_char:
            for row, index_to_box in rows:
                re_matches = re.findall(re_char, row)
                # print(row)
                if re_matches:
                    indexes = [i.start() for i in re.finditer(re_char, row)]
                    anchor_boxes = [index_to_box[x + len(re_matches[i]) - 1] for i, x in enumerate(indexes)]
                    self._match_row_keys(anchor_boxes, indexes, re_matches, index_to_box)

    def _match_column_keys(self, box, key, direction, re_pattern):
        text_boxes = [box for box in self.text_boxes if box.can_extract()]
        if isinstance(direction, tuple):
            values = []
            for x in direction:
                values.extend(box.filter_by_direction(text_boxes, x))
        else:
            values = box.filter_by_direction(text_boxes, direction)
        values_boxes = []
        for x in values:
            if re.findall(re_pattern, x.text):
                values_boxes.append(x)
                x.extract_flag = True

        ###############   建行  工本费/转账汇款手续费/手续费 金额  ###############
        if key == "工本费/转账汇款手续费/手续费金额":
            # print([x.text for x in values_boxes])
            # print(self._zip_by_columns(values_boxes))
            values, values_ = self._zip_by_columns(values_boxes)
            if "工本费/转账汇款手续费/手续费" in self.extract_info:
                self._update_extract_info({"工本费/转账汇款手续费/手续费_column": values})
            else:
                self._update_extract_info({"工本费/转账汇款手续费/手续费": values})
            if "金额" in self.extract_info:
                self._update_extract_info({"金额_column": values_})
            else:
                self._update_extract_info({"金额": values_})
        else:
            values_boxes = self._concat_row(values_boxes, True)
            if key in self.extract_info:
                info = {key + '_column': values_boxes}
            else:
                info = {key: values_boxes}
            box.extract_flag = True
            self._update_extract_info(info)

    def _extract_by_column_keys(self):
        for (key, direction, re_pattern) in column_keys[self.bank_type]:
            for box in self.text_boxes:
                if box.text == key:
                    self._match_column_keys(box, key, direction, re_pattern)

    def _extract_common(self):
        for box in self.text_boxes:
            for k, v in common.items():
                if box.can_extract() and re.findall(v, box.text):
                    box.extract_flag = True
                    self._update_extract_info({k: box.text})

    def _undefined_group(self):
        self.extract_info["undefined"] = [x.text for x in self.text_boxes if x.can_extract()]

    def extract(self):
        #  通过配置的已知字段进行提取
        rows = self._concat_row()
        self._extract_by_row_keys(rows)

        #  通过显式冒号进行通用提取
        self._extract_by_colon()

        #  通过配置的纵向字段进行提取
        self._extract_by_column_keys()

        #  常识提取
        self._extract_common()
        self._undefined_group()

        return self.extract_info


class ReceiptPage:
    def __init__(self, bank_type, page_id, page):
        self.bank_type = bank_type
        self.page_id = page_id
        self.receipt_id = 1
        self.words = [TextBox(**box, bank_type=bank_type) for box in page.extract_words()]
        self.receipt = {}
        self._cut()

    def _cut(self):
        temp = []
        title_flag = False
        titles_legal = "|".join(splits[self.bank_type])
        for x in self.words:
            if re.findall(titles_legal, x.text) and temp and title_flag:
                self.receipt["receipt_{}_{}".format(self.page_id, self.receipt_id)] = Receipt(self.bank_type,
                                                                                              self.page_id,
                                                                                              self.receipt_id,
                                                                                              temp)
                temp = [x]
                self.receipt_id += 1
                title_flag = True
            else:
                temp.append(x)
                if re.findall(titles_legal, x.text):
                    title_flag = True
        if temp and title_flag:
            self.receipt["receipt_{}_{}".format(self.page_id, self.receipt_id)] = Receipt(self.bank_type,
                                                                                          self.page_id,
                                                                                          self.receipt_id,
                                                                                          temp)

    def extract(self):
        self.receipt = {k: v.extract() for k, v in self.receipt.items()}
        self.receipt["code"] = "succeed"
        self.receipt["total"] = self.receipt_id
        return self.receipt


class ReceiptParse:
    def __init__(self, RequestId, ReceiptUrl, CompanyId, BankType, request_time):
        self.request_id = RequestId
        self.receipt_url = ReceiptUrl
        self.company_id = CompanyId
        self.bank_type = BankType
        self.parse_score_url = ""
        self.pdf = None
        self.request_time = request_time
        self.pages = {}
        self.extract_score = {}

    def _read_pdf(self):
        response = req.get(self.receipt_url)
        return pdfplumber.open(BytesIO(response.content))

    def _new_pages(self):
        pages = {}
        boxes = []
        for index, page in enumerate(self.pdf.pages):
            receipt_page = ReceiptPage(self.bank_type, index + 1, page)
            pages["page_{}".format(index + 1)] = receipt_page
            boxes += receipt_page.words
        if not boxes and len(self.pdf.pages):
            raise Exception("The current version does not support picture type PDF")
        return pages

    @debug
    def extractor(self, callback_url):
        self.pdf = self._read_pdf()
        self.pages = self._new_pages()
        self.extract_score = {k: v.extract() for k, v in self.pages.items()}
        # print(self.extract_score)
        self._save_to_ufile()
        self.call_back(callback_url)
        return self.extract_score

    @debug
    def _save_to_ufile(self):
        # 二进制数据流
        bio = BytesIO(json.dumps(self.extract_score).encode('utf-8'))
        # 上传数据流在空间中的名称
        stream_key = self.request_id
        ret, resp = putufile_handler.putstream(ufile_bucket_Name, stream_key, bio)
        assert resp.status_code == 200
        self.parse_score_url = ufile_url.format(ufile_bucket_Name, region, stream_key)
        # print(self.parse_score_url)

    @debug
    def _post_message(self, info, callback_url):
        headers = {'content-type': 'application/json'}
        resp = requests.post(callback_url.format(self.company_id), json=info, headers=headers)
        logger.warning("receipt_url:{}".format(self.receipt_url))
        logger.warning("info:{}".format(info))
        logger.warning("resp:{}".format(resp))
        logger.warning("cost:{}".format(int(time.time()) - self.request_time))

    def call_back(self, callback_url, exception=None):
        info = {"RequestId": self.request_id,
                "CompanyId": self.company_id,
                "BankType": self.bank_type}
        if exception:
            info["Code"] = "failed"
            info["Exception"] = exception
            info["ParseScoreUrl"] = None
        else:
            info["Code"] = "succeed"
            info["Exception"] = None
            info["ParseScoreUrl"] = self.parse_score_url
        self._post_message(info, callback_url)
