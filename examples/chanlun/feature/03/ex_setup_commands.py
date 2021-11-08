# coding=utf-8

from pytdx.parser.base import BaseParser
from pytdx.helper import get_datetime, get_volume, get_price
from collections import OrderedDict
import struct


class ExSetupCmd1(BaseParser):

    def setup(self):
        # self.send_pkg = bytearray.fromhex("01 01 48 65 00 01 52 00 52 00 54 24 1f 32 c6 e5"
        #                                     "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 1f 32 c6 e5"
        #                                     "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 1f 32 c6 e5"
        #                                     "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 1f 32 c6 e5"
        #                                     "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 cc e1 6d ff"
        #                                     "d5 ba 3f b8 cb c5 7a 05 4f 77 48 ea")
        self.send_pkg = bytearray.fromhex("01 01 48 65 00 01 52 00 52 00 54 24"
                                          "FC F0 0E 92 F3 C8 37 83 1F 32 C6 E5 D5 3D FB 41 CD 9C"
                                          "F2 07 FC D0 3C F6 F2 F7 A4 77 47 83 1D 59 9D CC 1F 91"
                                          "D5 55 82 DC 09 07 EE 29 DD FE 4C 28 1F 32 C6 E5 D5 3D"
                                          "FB 41 1F 32 C6 E5 D5 3D FB 41 F3 43 87 E6 68 A9 2A A3"
                                          "70 11 E4 9C D2 6E B0 1A")
    def parseResponse(self, body_buf):
        pass
