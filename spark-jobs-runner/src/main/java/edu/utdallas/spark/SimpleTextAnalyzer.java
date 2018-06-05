package edu.utdallas.spark;


import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

public class SimpleTextAnalyzer {

    private List<Keyword> keywords = new ArrayList<>();
    private List<NamedEntity> namedEntities = new ArrayList<>();
    public List<Keyword> getKeywords() {
        return keywords;
    }
    public List<NamedEntity> getNamedEntities() {
        return namedEntities;
    }

    private String[] stp = {"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the","+",",",".","-","'","\"","&","!","?",":",";","#","~","=","/","$","£","^","(",")","_","<",">"};
    private List<String> stopWords = Arrays.asList(stp);

    String[] tags = {
            //"FW", // Foreign Word
            "JJ", // Adjectives
            "JJR",
            "JJS",
            "NN", // Nouns
            "NNS",
            "NNP", // Proper nouns
            "NNPS",
            "RB", // adverbs
            "RBR",
            "RBS",
            "VB", // Verbs
            "VBZ",
            "VBP",
            "VBD",
            "VBN",
            "VBG"
    };

    List<String> keepTags = Arrays.asList(tags);

    public StanfordCoreNLP createPipeline() {

        System.out.println("Creating parser pipeline...");

        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        return pipeline;
    }

    /**
     * Analyze given text. After analysis, the keywords and named entities can be accessed
     * with getNameEntities() and getKeywords() methods.
     * @param text
     */
    private void analyze(String text, StanfordCoreNLP pipeline) {

        keywords.clear();

        List<Token> tokens = new ArrayList<>();

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // run all Annotators on this text
        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            try {
                processSentence(sentence, tokens);
            }
            catch (Exception e){
                System.out.println("TextAnalyzer: unable to process sentence: "+ e.getClass() + " :: " +e.getMessage() );
                e.printStackTrace();
            }
        }


        for (Token t : tokens){

            if (!keepTags.contains(t.getPosTag()) || stopWords.contains(t.getLemma())){
                continue;
            }

            Keyword k = new Keyword();

            k.setWord(t.getLemma());
            k.setWeight(t.getOccurences());

            keywords.add(k);
        }

    }

    private void processSentence(CoreMap s, List<Token> keywords){

    for (CoreLabel rawWord: s.get(CoreAnnotations.TokensAnnotation.class)) {

            Token w = new Token();

            w.setWord(rawWord.get(CoreAnnotations.TextAnnotation.class));
            w.setLemma(rawWord.get(CoreAnnotations.LemmaAnnotation.class));
            w.setPosTag(rawWord.get(CoreAnnotations.PartOfSpeechAnnotation.class));
            w.setOccurences(1);

            keywords.add(w);
        }
    }

    public HashMap<String, Integer> extractKeywords(String text, StanfordCoreNLP pipeline){
        analyze(text, pipeline);

        ArrayList<Word> words = new ArrayList<>();
        words.addAll(getKeywords());
        words.addAll(getNamedEntities());

        HashMap<String, Integer> keywords = new HashMap<>();

        for (Word w : words){
            String word = w.getWord();
            if (!keywords.containsKey(word)){
                keywords.put(word, w.getWeight());
            }
            else {
                keywords.put(word, keywords.get(word) + w.getWeight());
            }
        }

        return keywords;
    }


    /**
     * Test driver.
     * @param args
     */
    public static void main (String[] args){
        System.out.println("Analyzing text:");
        String text = "Donald Trump is running as a republican to be President. Donald is going to win. Learn learning learned learns. The history of NLP generally starts in the 1950s, although work can be found from earlier periods. In 1950, Alan Turing published an article titled \"Computing Machinery and Intelligence\" which proposed what is now called the Turing test as a criterion of intelligence.\n" +
                "\n" +
                "The Georgetown experiment in 1954 involved fully automatic translation of more than sixty Russian sentences into English. The authors claimed that within three or five years, machine translation would be a solved problem.[2] However, real progress was much slower, and after the ALPAC report in 1966, which found that ten-year-long research had failed to fulfill the expectations, funding for machine translation was dramatically reduced. Little further research in machine translation was conducted until the late 1980s, when the first statistical machine translation systems were developed.\n" +
                "\n" +
                "Some notably successful NLP systems developed in the 1960s were SHRDLU, a natural language system working in restricted \"blocks worlds\" with restricted vocabularies, and ELIZA, a simulation of a Rogerian psychotherapist, written by Joseph Weizenbaum between 1964 and 1966. Using almost no information about human thought or emotion, ELIZA sometimes provided a startlingly human-like interaction. When the \"patient\" exceeded the very small knowledge base, ELIZA might provide a generic response, for example, responding to \"My head hurts\" with \"Why do you say your head hurts?\".\n" +
                "\n" +
                "During the 1970s many programmers began to write 'conceptual ontologies', which structured real-world information into computer-understandable data. Examples are MARGIE (Schank, 1975), SAM (Cullingford, 1978), PAM (Wilensky, 1978), TaleSpin (Meehan, 1976), QUALM (Lehnert, 1977), Politics (Carbonell, 1979), and Plot Units (Lehnert 1981). During this time, many chatterbots were written including PARRY, Racter, and Jabberwacky.\n" +
                "\n" +
                "Up to the 1980s, most NLP systems were based on complex sets of hand-written rules. Starting in the late 1980s, however, there was a revolution in NLP with the introduction of machine learning algorithms for language processing. This was due to both the steady increase in computational power (see Moore's Law) and the gradual lessening of the dominance of Chomskyan theories of linguistics (e.g. transformational grammar), whose theoretical underpinnings discouraged the sort of corpus linguistics that underlies the machine-learning approach to language processing.[3] Some of the earliest-used machine learning algorithms, such as decision trees, produced systems of hard if-then rules similar to existing hand-written rules. However, Part-of-speech tagging introduced the use of Hidden Markov Models to NLP, and increasingly, research has focused on statistical models, which make soft, probabilistic decisions based on attaching real-valued weights to the features making up the input data. The cache language models upon which many speech recognition systems now rely are examples of such statistical models. Such models are generally more robust when given unfamiliar input, especially input that contains errors (as is very common for real-world data), and produce more reliable results when integrated into a larger system comprising multiple subtasks.\n" +
                "\n" +
                "Many of the notable early successes occurred in the field of machine translation, due especially to work at IBM Research, where successively more complicated statistical models were developed. These systems were able to take advantage of existing multilingual textual corpora that had been produced by the Parliament of Canada and the European Union as a result of laws calling for the translation of all governmental proceedings into all official languages of the corresponding systems of government. However, most other systems depended on corpora specifically developed for the tasks implemented by these systems, which was (and often continues to be) a major limitation in the success of these systems. As a result, a great deal of research has gone into methods of more effectively learning from limited amounts of data.\n" +
                "\n" +
                "Recent research has increasingly focused on unsupervised and semi-supervised learning algorithms. Such algorithms are able to learn from data that has not been hand-annotated with the desired answers, or using a combination of annotated and non-annotated data. Generally, this task is much more difficult than supervised learning, and typically produces less accurate results for a given amount of input data. However, there is an enormous amount of non-annotated data available (including, among other things, the entire content of the World Wide Web), which can often make up for the inferior results.";
        System.out.println(text);

        SimpleTextAnalyzer a = new SimpleTextAnalyzer();
        StanfordCoreNLP pipeline = a.createPipeline();

        a.analyze(text, pipeline);


        ArrayList<Word> words = new ArrayList<>();
        words.addAll(a.getKeywords());
        words.addAll(a.getNamedEntities());

        HashMap<String, Integer> keywords = new HashMap<>();

        for (Word w : words){
            String word = w.getWord();
            if (!keywords.containsKey(word)){
                keywords.put(word, w.getWeight());
            }
            else {
                keywords.put(word, keywords.get(word) + w.getWeight());
            }
        }

        for (String k : keywords.keySet()){
            System.out.println(k + " - " + keywords.get(k));
        }
    }

}


