package com.cai.test;


import java.util.ArrayList;
import java.util.List;

public class Combination {
    void combination(String s) {
        char[] strs = s.toCharArray();
        int n = s.length();
        int nbit = 1 << n;
        for (int i = 0; i < nbit; i++) {//一共nbit个，i对应输出的第i+1个，因为从0开始的；
            for (int j = 0; j < n; j++) {
                int tmp = 1 << j;
                if ((tmp & i) != 0) { // 1&3 == 2, ...
                    System.out.print(strs[j]);
                }
            }
            System.out.println();
        }
        System.out.println("result num is : " + (nbit - 1));
    }

    public void p(char[] arrays, int begin, int number, List<Character> list){

        if(number==0){
            System.out.println(list.toString());
            return;
        }
        if(begin==arrays.length){
            return;
        }
        list.add(arrays[begin]);
        p(arrays, begin+1, number-1,list);
        list.remove((Character)arrays[begin]);
        p(arrays, begin+1, number,list);
    }


    public static void main(String[] args) {
        new Combination().combination("abcde");
        char[] arrays={'a','b','c', 'd', 'e'};
        Combination zuhe=new Combination();
        List<Character> list= new ArrayList();
        for(int number=1;number<=arrays.length;number++) {
            zuhe.p(arrays, 0, number, list);
        }
    }
}
