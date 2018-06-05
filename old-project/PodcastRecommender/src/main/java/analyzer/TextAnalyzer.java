package analyzer;

import java.util.*;

//import com.atlassian.clover.reporters.html.source.SourceTraverser;
import edu.stanford.nlp.simple.*;
import model.Keyword;
import model.NamedEntity;
import model.Word;

/**
 * Class used to analyze text and extract keywords and named entities.
 */
public class TextAnalyzer {

    private List<NamedEntity> namedEntities = new ArrayList<>();
    private List<Keyword> keywords = new ArrayList<>();

    public List<NamedEntity> getNamedEntities() {
        return namedEntities;
    }
    public List<Keyword> getKeywords() {
        return keywords;
    }

    /**
     * Analyze given text. After analysis, the keywords and named entities can be accessed
     * with getNameEntities() and getKeywords() methods.
     * @param text
     */
    public void analyze(String text) {

        namedEntities.clear();
        keywords.clear();

        List<Token> tokens = new ArrayList<>();
        List<Token> NNPTokens = new ArrayList<>();
        List<Token> NNTokens = new ArrayList<>();


        Document document = new Document(text);
        for (int k = 0; k < document.sentences().size(); k++){
            Sentence s = document.sentence(k);

            try {
                processSentence(s, k, tokens);
            }
            catch (Exception e){
                System.out.println("TextAnalyzer: unable to process sentence: "+ e.getClass() + " :: " +e.getMessage() );
                e.printStackTrace();
            }
        }

        for (Token t : tokens){

            if (t.getNerTag().compareTo("O") != 0
                    || (t.getPosTag().compareTo("CD") == 0 && t.isCapital()))
            {
                boolean matched = false;
                for (Token nnp : NNPTokens){
                    // find an already processed proper name token that is
                    // a superset of current token -- e.g. t = "John" and nnp = "John Siracusa".
                    if (nnp.components.size() >= t.components.size()
                            && (nnp.partialMatch(t) || nnp.totalMatch(t)))
                    {
                        nnp.setOccurences(nnp.getOccurences() + 1);
                        matched = true;
                        break;
                    }
                }
                // if proper name is not matched, then it is a new name.
                if (!matched){
                    t.setOccurences(1);
                    NNPTokens.add(t);
                }
            }
            else {
                Boolean matched = false;
                for (Token nn : NNTokens){
                    if (nn.totalMatch(t)){
                        nn.setOccurences(nn.getOccurences() + 1);
                        matched = true;
                        break;
                    }
                }
                if (!matched){
                    t.setOccurences(1);
                    NNTokens.add(t);
                }
            }
        }

        for (Token t : NNPTokens){

            if ((t.getNerTag().compareTo("PERSON") == 0)
                && t.toString().length() >= 5)
            {

                String[] substrings = t.toString().split("\\.");

                for (String substring : substrings) {
                    NamedEntity e = new NamedEntity();

                    e.setName(substring);
                    e.setWeight(t.getOccurences());
                    e.setVip(false);

                    namedEntities.add(e);
                }
            }
        }

        for (Token t : NNTokens){
            String[] substrings = t.toString().split("\\.");

            for (String substring : substrings) {
                substring = t.toString().trim();
                if (substring.length() >= 4) {
                    Keyword k = new Keyword();

                    k.setWord(substring);
                    k.setWeight(t.getOccurences());

                    keywords.add(k);
                }
            }
        }
    }

    private static Token getWord(int i, Sentence s, int k){
        Token w = new Token();

        w.setSentenceId(k);
        w.setWordId(i);
        w.setWord(s.words().get(i));
        w.setPosTag(s.posTag(i));
        w.setNerTag(s.nerTag(i));
        w.setLemma(s.lemma(i));

        return w;
    }

    private static void processSentence(Sentence s, Integer k, List<Token> keywords){
        Deque<Token> componentStack = new ArrayDeque<>();

        List<String> rawWords = s.words();
        for (int i = 0 ; i < rawWords.size(); i++){

            Token w = new Token();

            w.setSentenceId(k);
            w.setWordId(i);
            w.setWord(rawWords.get(i));
            w.setPosTag(s.posTag(i));
            w.setNerTag(s.nerTag(i));
            w.setLemma(s.lemma(i));


            switch (w.getPosTag()){
                case "NN": // noun
                case "NNP": // proper noun
                case "NNPS": // proper plural noun
                case "VBG":// gerund, e.g. "podcasting"


                    if (componentStack.size() > 0){
                        // last component is same part of speech -- assume they are a unit
                        if (componentStack.peekLast().getPosTag().compareTo(w.getPosTag()) == 0){
                            componentStack.push(w);
                        }
                        else { // current word is different pos
                            Token component = componentStack.removeLast();
                            while (!componentStack.isEmpty()){
                                component.appendComponent(componentStack.removeLast());
                            }
                            keywords.add(component);
                        }
                    }
                    else {
                        componentStack.push(w);
                    }

                    break;
                default:

                    if (componentStack.size() > 0){
                        Token component = componentStack.removeLast();
                        while (!componentStack.isEmpty()){
                            component.appendComponent(componentStack.removeLast());
                        }
                        keywords.add(component);
                    }

                    break;

            }
        }
    }

    /**
     * Test driver.
     * @param args
     */
    public static void main (String[] args){
        System.out.println("Analyzing text:");
        String text = "Donald Trump is running as a republican to be President.";
        text = "The history of NLP generally starts in the 1950s, although work can be found from earlier periods. In 1950, Alan Turing published an article titled \"Computing Machinery and Intelligence\" which proposed what is now called the Turing test as a criterion of intelligence.\n" +
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
                TextAnalyzer a = new TextAnalyzer();
        System.out.println(text);
        a.analyze(text);
        System.out.println("\nRESULTS:");
        System.out.println("Keywords:");
        for (Keyword k : a.getKeywords()){
            System.out.println(k);
        }
        System.out.println("\nNamed Entities:");
        for (NamedEntity e : a.getNamedEntities()){
            System.out.println(e);
        }

    }
}
