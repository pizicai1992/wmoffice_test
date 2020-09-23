# -*- coding: UTF-8 -*-

# 汉诺塔问题：
# 问题：https://baike.baidu.com/item/%E6%B1%89%E8%AF%BA%E5%A1%94/3468295?fr=aladdin 
# 思路：
# 当n=3时，需要7步才能移动到另外一个柱子，
# 当n=4时，首先把上面的3个先动移动另一个柱子上（需要7步），然后把原先底层最大的圆盘挪到剩余的空柱子上（这需要一步），
# 这时候再把原先挪走的那3个圆盘移动到最大的圆盘上面，由于最大的圆盘已经在最下面，所以在移动这3个圆盘时没有任何顾虑，
# 此时还需要7步才能完成，所以最终步骤数是 7+1+7 = 15步
# 抽象后 即 f(n) = 2*f(n-1) + 1

def hanNuoTa(n):
    if n == 1:
        return 1 
    elif n > 1:
        return 2 * (hanNuoTa(n-1)) + 1

print hanNuoTa(5)