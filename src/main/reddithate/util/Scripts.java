package reddithate.util;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.commons.codec.Charsets;
import reddithate.util.StringProcessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Scripts
{
    public static void main( String[] args ) throws IOException
    {
        String[] input = { "authorbyhatebywordcountcomments", "authorbyhatebywordcountsubmissions" };
        String[] output = { "nauthorbyhatebywordcountcomments", "nauthorbyhatewordcountsubmissions" };

        for ( int i = 0; i < input.length; i++ )
        {
            BufferedReader br = Files.newBufferedReader( Paths.get( input[i] ), Charsets.UTF_8 );
            BufferedWriter bw = Files.newBufferedWriter( Paths.get( output[i] ), Charsets.UTF_8 );

            String line;
            while ((line = br.readLine()) != null)
            {
                String[] tokens = line.split( "\\t" );
                if ( !RussianTrollNames.isTroll( tokens[0] ) )
                {
                    bw.write( line + System.lineSeparator() );
                }
            }

            bw.close();
            br.close();
        }


//        List<String> output = new ArrayList<>();

//        for ( String line : lines )
//        {
//            Any s = JsonIterator.deserialize( line );
//            System.out.println( toHourMonthYear( Long.parseLong( s.get( "created_utc" ).toString() ) ) );
//            System.out.println( toYearMonthDay( Long.parseLong( s.get( "created_utc" ).toString() ) ) );
//        }
//
//        System.out.println( toHourMonthYear( 1539392400 ) );
//        System.out.println( toYearMonthDay( 1539392400 ) );
//
//        System.out.println( StringProcessing.stem( "does, this; remove stop words like like the are! " ) );
//        List<String> outputToBeFiltered = Arrays.asList( "authorbyhatebywordcountcomments",
//                                                         "authorbyhatebywordcountsubmissions",
//                                                         "subredditbyhatebywordcountcomments",
//                                                         "subredditbyhatebywordcountsubmissions" );
//        for ( String f : outputToBeFiltered )
//        {
//            filterZerosAndOutput( f );
//        }
    }

    public static void filterZerosAndOutput( String filename ) throws IOException
    {
        BufferedWriter bw = Files.newBufferedWriter( Paths.get( "filtered_" + filename ) );
        BufferedReader br = Files.newBufferedReader( Paths.get( filename ), Charsets.UTF_8 );

        String line;
        while ( ( line = br.readLine() ) != null )
        {
            String[] split = line.split( "\t" );
            if ( !split[1].equals( "0.0" ) )
            {
                bw.write( line + "\n" );
            }
        }

        br.close();
        bw.close();
    }


    public static String toHourMonthYear( long utc )
    {
        // hh yyyy MM e.g., 13 2017 07, where 13 is 24-hour time and 07 is month
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond( utc, 0, ZoneOffset.UTC );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "kk MM yyyy" );
        return dateTime.format( formatter );
    }

    public static String toYearMonthDay( long utc )
    {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond( utc, 0, ZoneOffset.UTC );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "yyyy MM dd" );
        return dateTime.format( formatter );
    }

    public static void getStemmedHatewords() throws IOException
    {
        String[] hate = { "abachabu", "abagima", "abaisia", "abazungu", "abbo", "abd", "abeed", "abelungu", "abid",
                          "abo",
                          "africoon", "ahab", "ahmadiyah", "ainu", "ajam", "ajayee", "akata", "akhbaroshim", "alaman", "alligator bait", "allochtoon", "amakwerekwere", "ambattar", "ame koh", "americoon", "amerikkkan", "ami", "arabush", "aracuano", "arapis", "argie", "armo", "arsch", "arschgeburt", "arschloch", "asylschmarotzer", "avalivavandu", "avaseeve", "azungu", "badugudugu", "bai tou", "baijo", "baitola", "bak guiy", "balija", "baluba", "bamboo coon", "banana bender", "banana lander", "bangla", "banjo lips", "bans and cans", "basungu", "bazi", "bazungu", "beach nigger", "bean dipper", "beaner", "beaner shnitzel", "beaney", "belegana", "berberaap", "bergie", "beur", "beurette", "bhrempti", "biba", "bicha", "bichinha", "bint", "bitch", "bitter clinger", "bix nood", "black dago", "blaxican", "bluegum", "boche", "boffer", "bog hopper", "bog irish", "bog jumper", "bog trotter", "bohunk", "boiola", "bolillo", "boofer", "boojie", "book book", "booner", "boong", "boonga", "bor", "border bunny", "border hopper", "border jumper", "border nigger", "bosche", "boskur", "bosyanju", "bougnoule", "boxhead", "bozgor", "brass ankle", "buckethead", "buckra", "buddhahead", "budos olah", "buffie", "bugre", "buk buk", "bule", "buleh", "bulgaroskopian", "bume", "bung", "bunga", "burrhead", "butterhead", "buzi", "cab nigger", "cabezita negra", "caboclo", "caboco", "caco", "cacorro", "caffre", "cafuzo", "camel cowboy", "camel fucker", "camel humper", "camel jacker", "camel jockey", "can eater", "carcamano", "carpet pilot", "carrot snapper", "casado", "caublasian", "cave nigger", "cefur", "cerrero", "cetnik", "chach chach", "chakh chakh", "chalckiliyar", "chale", "champinon", "chan koro", "chankoro", "chapat", "chapata", "chapeton", "chapetona", "chapin", "chapina", "chapta", "charlie", "charnega", "charnego", "charva", "charver", "chav", "chee chee", "cheena", "cheese eating surrender monkey", "chefur", "chekwa", "chele", "chelo", "chernozhopiy", "cheuchter", "chi chi", "chigger", "chilango", "chili shitter", "chinaman", "chinese wetback", "chinetoque", "ching chong", "chingeleshi", "chinig", "chinja chinja", "chink", "chink a billy", "chizungu", "chleuh", "cholero", "cholo", "chon", "chon koh", "chonky", "choochter", "chorik", "chosenjin", "christ killer", "chuchter", "chupta", "ciapak", "ciapata", "ciapaty", "cioara", "clamhead", "cocolo", "coolie", "coon", "coon ass", "cotton picker", "cow kisser", "cowboy killer", "crucco", "culona inchiavabile", "cultuurverrijker", "cunt", "curry muncher", "curry slurper", "curry stinker", "cushi", "cushite", "dago", "dambaya", "darkey", "darkie", "darky", "ddang kong", "dego", "demala", "dhimmi", "diaper head", "dinge", "dingo fucker", "dink", "disminuidos", "ditsoon", "dogan", "dogun", "domes", "doryphore", "doss", "dot head", "dune coon", "dune nigger", "dyke", "dyke jumper", "eh hole", "el abeed", "emoit", "escuaca", "eurotrash", "eyetie", "fag", "faggot", "fan kuei", "fanook", "faranji", "fashisty", "fenucca", "ficker", "filhos da terra", "finook", "firangi", "fob", "fog nigger", "fotze", "fresh off the boat", "fritsove", "froschschenkelfresser", "fryc", "fryce", "gabacho", "gable", "gaiko", "gator bait", "gaurkh", "geitenneuker", "gender bender", "gerudo", "gew", "ghetto hamster", "giaour", "gin jockey", "ginzo", "gipp", "gippo", "glatze", "gokiburi", "golliwog", "gook", "gook eye", "gooky", "gora", "goy", "goyim", "goyum", "greaseball", "gringa", "gringo", "groid", "guajiro", "guala", "guala guala", "gub", "gubba", "guera", "guero", "guerro", "guido", "guizi", "gummihals", "gun burglar", "gurrier", "gwailo", "gwat", "gweilo", "gyp", "gypo", "gyppie", "gyppo", "gyppy", "haatmeneer", "hadji", "hagwei", "hairyback", "haji", "hajji", "halfrican", "hamba", "hambaya", "han nichi", "hans", "haole", "hapankaali", "hapshi", "hayquay", "hayseed", "hebro", "heeb", "heinie", "hick", "higger", "hitomodoki", "ho", "homppeli", "honkey", "honkie", "honky", "honyak", "honyock", "house nigger", "hunkie", "hunky", "hunni", "hunyak", "hunyock", "hure", "hurensohn", "hymie", "ice monkey", "ice nigger", "ike", "ikey", "ikey mo", "ikizungu", "iky", "imeet", "impedido", "incapaz", "indio", "indio ladino", "indon", "injun", "inselaffen", "intsik", "invalidos", "inyenzi", "island nigger", "ita koh", "jabonee", "jant", "japana", "japie", "japse", "jareer", "jathi", "jewbacca", "jhant", "jibaro", "jigaboo", "jigarooni", "jigg", "jigga", "jiggabo", "jiggaboo", "jigger", "jihadi", "jijjiboo", "jim fish", "jincho", "jincho papujo", "jokuoye", "judensau", "judenschwein", "jungle bunny", "kaaskop", "kabisi", "kabloonuk", "kaeriya", "kaffer", "kaffre", "kafir", "kafiri", "kala", "kalar", "kalla", "kallathoni", "kallathonni", "kalmuk", "kalmuki", "kamelenneuker", "kanacke", "kanaker", "kansarme", "karaiyar", "katol", "katzenfresser", "keling", "khazar", "kihii", "kiingereza", "kijuju", "kike", "kinderficker", "kizungu", "klandestin", "knacker", "kochchiya", "kokujin", "komunjara", "kopvoddrager", "kotiya", "kraut", "krueppel", "kuffar", "kurombo", "kurwa", "kushi", "kushite", "kut-marokkaan", "kutmarokkaan", "kwai lo", "kwerekwere", "kweri kweri", "kwiri kwiri", "kyke", "labu", "lagartona", "laikci", "lamemurata", "landya", "langsiya", "lansiya", "laowai", "latrino", "lawn jockey", "leb", "lebbo", "lesbo", "limey", "ling ling", "longuu", "longuulkitkit", "lowlander", "lubra", "lugan", "lugenpresse", "lungereza", "macaca", "macaco", "macedonist", "macengi", "mackerel snapper", "maekka", "maessa", "makaronifresser", "malaun", "maldito bori", "malingsia", "mameluco", "mangia cake", "mangiacrauti", "mangiapatate", "maricon", "marrano", "marron", "mattaya", "mayate", "medelander", "meiguo guizi", "melanzana", "merkin", "mestico", "mestizo", "mezza fanook", "mick", "mickey finn", "mil bag", "millie", "minusvalido", "missgeburt", "mithangel", "moch", "mocho", "mockey", "mockie", "mocky", "mocro", "modaya", "moelander", "mof", "moffen", "mogolico", "moher", "mojado", "moke", "moky", "mollo", "mong", "mongolo", "monser", "mook", "mooley", "mooliachi", "moolie", "moon cricket", "moor", "moreno", "moro", "moss eater", "moulie", "moulignon", "moulinyan", "mud duck", "mud person", "mud shark", "mudugudugu", "muktuk", "mulato", "mulignan", "mung", "munt", "munter", "murungu", "mustalainen", "mustalaiset", "musungu", "muttal", "muzungu", "muzzie", "mwiji", "mzungu", "naca", "naco", "natsi", "natzisty", "neche", "neechee", "neejee", "neger", "negro de mierda", "nemcurji", "nemskutarji", "neres", "net head", "newfie", "ngetik", "nguoi tau", "nicca", "nichi", "nichiwa", "nidge", "niemra", "niemry", "nig", "nig nog", "nigar", "nigette", "nigga", "niggah", "niggar", "nigger", "nigglet", "niggor", "niggress", "nigguh", "niggur", "niglet", "nigor", "nigra", "nigre", "niknok", "niksmann", "nitchee", "nitchie", "nitchy", "northern monkey", "ocker", "octaroon", "octroon", "ofay", "ola", "orangie", "osuuji", "otuwa", "oven dodger", "paddy", "pakeha", "pakka", "paleface", "palla", "pancake face", "papist", "papoose", "paraiyar", "parangi", "pato", "payoponi", "peckerwood", "peela", "pendatang", "penner", "pepik", "perra", "perroflauta", "pickaninny", "piefke", "piffke", "piker", "pikey", "piky", "pinche negro", "pindos", "pineapple nigger", "ping pang", "plastic paddy", "pocha", "pocho", "poebe", "pogue", "pohm", "polack", "polacos", "polake", "popolo", "poppadom", "porch monkey", "powderburn", "prairie nigger", "preiss", "prieto", "proddy dog", "proddywhoddy", "proddywoddy", "prusak", "prusatsi", "pseudomacedonian", "puta", "putain", "pute", "puto", "quadroon", "quashie", "race traitor", "raghead", "razakars", "redlegs", "retard", "retarded", "retrasado mental", "rhine monkey", "riben guizi", "ricepicker", "rifaap", "rifaapje", "rital", "roofucker", "rooinek", "rosbif", "rosuke", "roundeye", "rube", "rubwa", "ruco", "russellite", "rutuku", "saeedi", "sakkiliya", "sakkiliyar", "saks", "salope", "sambo", "sand nigger", "sapatao", "sart", "sauschwob", "sawcsm", "sawney", "sayeedi", "sayoku", "scallie", "scanger", "scheiss ami", "schiptar", "schlampe", "schlitzauge", "schvartse", "schwanzlutscher", "schwartze", "schwarze", "schwarzer", "schwuchtel", "scobe", "scuffer", "semihole", "senga", "seppo", "sesat", "shagitz", "shahktor", "shahktyer", "shanty irish", "sheeny", "sheepfucker", "sheigetz", "sheister", "shelta", "shemale", "shiksa", "shit heel", "shit kicker", "shkutzim", "shvartz", "shylock", "shyster", "sideways cooter", "sideways pussy", "sideways vagina", "skag", "skanger", "skinhead", "skopiana", "skopianika", "skopjan", "skopjian", "slampa", "slant eye", "slopehead", "slopy", "smick", "smoke jumper", "smoked irish", "smoked irishman", "sokac", "sokomokabul", "soqi", "soup taker", "southern fairy", "spast", "spasti", "spear chucker", "sperg", "spic", "spice nigger", "spick", "spickaboo", "spide", "spig", "spigger", "spigotty", "spik", "spink", "spiv", "squarehead", "squaw", "steek", "stovepipe", "stump jumper", "sub human", "sudaca", "sudda", "surrender monkey", "svab", "svaba", "szkop", "szwab", "tabeetsu", "taig", "tapette", "tapori", "tar baby", "teague", "teg", "teig", "tek millet", "tenker", "tete carree", "teuchtar", "teuchter", "thalaya", "thambiya", "thicklips", "thurumba", "timber nigger", "tincker", "tinkar", "tinkard", "tinkere", "tizzone", "tizzoni", "touch of the tar brush", "towel head", "trailer park trash", "trailer trash", "tranny", "trucin", "tschusch", "tsekwa", "tugo", "tunte", "tyncar", "tynekere", "tynkard", "tynkare", "tynker", "tynkere", "ubangee", "ubangi", "umlungu", "umuzungu", "uncircumcised baboon", "uncle tom", "untermensch", "untermenschen", "uriti ja mutigania wa wa kunati", "ustasa", "uzko glaziye", "varungu", "velcro head", "viado", "vlah", "wagon burner", "wazungu", "wetback", "wexican", "whigger", "white nigger", "white trash", "whitey", "whore from fife", "wic", "wigga", "wigger", "wiggerette", "wixer", "wog", "wop", "yardie", "yellow bone", "yid", "yob", "yobbo", "yokel", "yom", "youn", "yuon", "zambaggoa", "zambo", "zandneger", "zecke", "ziegenficker", "zigabo", "zigeuner", "zionazi", "zipperhead", "zippohead", "zog", "zog lover", "zorra" };

        List<String> stemmed = new ArrayList<>();

        for ( String s : hate )
        {
            List<String> tokens = StringProcessing.stem( s );
            StringBuilder sb = new StringBuilder();
            for ( String token : tokens )
            {
                sb.append( token ).append( " " );
            }

            sb.setLength( sb.length() - 1 );

            stemmed.add( sb.toString() );
        }

        StringBuilder sb = new StringBuilder();
        sb.append( "{" );
        for ( String s : stemmed )
        {
            sb.append( "\"" ).append( s ).append( "\"," );
        }

        sb.setLength( sb.length() - 1 );
        sb.append( "}" );


        System.out.println( sb.toString() );

    }
}
