package util;

import java.util.Arrays;

public class cleanwords {

	public static String cleanStopWords(String word) {
		if (!Arrays.asList(constantholder.stopwords).contains(word)) {
			return word;
		} else {
			return null;
		}
	}

	public static String stemmer(String word) {
		if (word != null) {
			stemmer s = new stemmer();
			return s.apply(word);
		} else {
			return null;
		}

	}

	public static String convertCase(String word) {
		if (word != null && word.length() > 0) {
			return word.toLowerCase();
		} else {
			return null;
		}
	}

	private static String removeSpecialChars(String word) {
		if (word != null && word.length() > 0) {
			word = word.replaceAll(
					"(http|https|ftp|gopher|telnet|file|Unsure|www).*", "");
			return word.replaceAll("[\\W_]", "");
		} else {
			return null;
		}
	}

	public static String cleaner(String word, String checkChar) {
		boolean check = (checkChar != null && checkChar.length() > 0) ? true
				: false;
		if (word != null) {
			word = convertCase(word);
		} else {
			return null;
		}
		if (word != null) {
			if (check) {
				if(word.startsWith(checkChar) && word.length() > 1){ 
					word = checkChar+ removeSpecialChars(word.substring(1));
				} else {
					return null;
				}
			} else {
				word = removeSpecialChars(word);
			}
		} else {
			return null;
		}
		if (word != null) {
			if (check) {
				if(word.startsWith(checkChar) && word.length() > 1){ 
					String cleanStr = cleanStopWords(word.substring(1));
					if(cleanStr == null) {
						return null;
					} 					
				} else {
					return null;
				}
			} else {
			word = cleanStopWords(word);
			}	
		} else {
			return null;
		}
		if (word != null) {
			if (check) {
				if(word.startsWith(checkChar) && word.length() > 1){ 
					word = checkChar+ stemmer(word.substring(1));
				} else {
					return null;
				}
			} else {
				word = stemmer(word);
			}
		} else {
			return null;
		}
		if (word != null) {
			if (check) {
				if(word.startsWith(checkChar)){ 
					if(word.substring(1).length() < 2) {
						return null;
					}
				}
			} else {
				if(word.length() < 2) {
					return null;
				}
			}
		} else {
			return null;
		}
		if (word != null) {
			if (check) {
				if(word.startsWith(checkChar)){ 
					if(word.substring(1).matches("[-+]?\\d*\\.?\\d+")) {
						return null;
					}
				}
			} else {
				if(word.matches("[-+]?\\d*\\.?\\d+")) {
					return null;
				}
			}
		} else {
			return null;
		}
		return word;
	}

	public static void main(String[] args) {
		String a = "rtdds3232@@@2sds";
		System.out.println(cleaner(a, null));

	}

}
