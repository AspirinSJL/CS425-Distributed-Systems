#! /usr/bin/env python

import matplotlib.pyplot as plt

def obama():
    message_id = [
        9,
        10,
        11,
        12,
        48,
        102,
        243,
        244,
        245,
        246,
        247,
        248,
        249,
    ]

    storm = [
        19,
        19,
        20,
        20,
        38,
        65,
        136,
        136,
        137,
        137,
        138,
        138,
        139,
    ]

    crane = [
        7,
        8,
        8,
        9,
        32,
        68,
        161,
        161,
        162,
        162,
        163,
        164,
        164,
    ]

    plt.plot(message_id, storm, label='Storm', marker='o')
    plt.plot(message_id, crane, label='Crane', marker='^')

    plt.legend(loc=0)
    plt.xlabel('message counter')
    plt.ylabel('time elapsed from beginning (s)')
    plt.title('Initial Delay of Crane and Storm')
    plt.show()

def avg():
    message_id = [
        100,
        200,
        500,
        1000,
        2000,
    ]

    storm = [
        62,
        112,
        262,
        513,
        1015,
    ]

    crane = [
        68,
        136,
        331,
        669,
        1330,
    ]

    plt.plot(message_id, storm, label='Storm', marker='o')
    plt.plot(message_id, crane, label='Crane', marker='^')

    plt.legend(loc=0)
    plt.xlabel('message counter')
    plt.ylabel('time elapsed from beginning (s)')
    plt.title('Performance of Crane and Storm')
    plt.show()

avg()