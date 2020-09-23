package com.cai.test;

public class ConcatString {
    public static void main(String[] args) {
        String word = "A12B23";
        //StringBuilder result_word = new StringBuilder();
        String result_word = "";
        for (int i = 0; i < word.length(); i++) {
            System.out.println(word.charAt(i));
            if (Character.isUpperCase(word.charAt(i))) {
                //result_word.append(',' + word.charAt(i) + ':');
                result_word += "," + word.charAt(i) + ":";
            }
            else {
                //result_word.append(word.charAt(i));
                result_word += word.charAt(i);
            }
        }
        System.out.println(result_word);
    }
}
