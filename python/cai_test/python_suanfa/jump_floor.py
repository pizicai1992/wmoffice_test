# -*- coding: UTF-8 -*-

# 青蛙跳台阶问题
# 一只青蛙一次可以跳上1级台阶，也可以跳上2级。求该青蛙跳上一个n级的台阶总共有多少种跳法。
# 思路：
# 其实是斐波那契数列的变形提，可以逆向思考，当跳到n-1级台阶时，如果在跳一步就到了n级，
# 如果跳到了n-2级台阶时，在跳两步就到了n级，所以可以理解为 f(n) = f(n-1) + f(n-2)

def jumpFloor(n):
    if n==1:
        return 1
    elif n == 2:
        return 2
    elif n > 2:
        return jumpFloor(n-1) + jumpFloor(n-2)

print jumpFloor(6)

# 问题进阶：如果青蛙每次不仅可以跳1次或2次，还可以跳3次、4次。。。到n次，求总的解法
# 思路：
# 跟原问题一样的思路，逆向思考，即如果已经跳到了n-m 阶，则在跳m次就到了n阶，总后还有一种跳法就是
# 直接从头跳n级 一步到顶，所以最后的解法就是
# f(n)=f(n-1)+f(n-2)+...+f(2)+f(1)+1 ,即最后结果是 2的 n-1次方

def jumpFloor2(n):
    if n==0:
        return 0
    elif n > 0:
        return 2 ** (n-1)
