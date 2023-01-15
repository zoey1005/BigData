import sys

# 输入为标准输入stdin
for line in sys.stdin:
    # 删除开头和结尾的空行
    line = line.strip()
    # 以默认空格分隔单词到words列表
    words = line.split()
    for word in words:
        # 输出所有单词，空格为"单词 1"以便作为reduce输入
        print("%s %s" % (word, 1))