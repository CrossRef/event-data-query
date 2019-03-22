(ns event-data-query.ingest-test
  (:require [clojure.test :refer :all]
            [event-data-query.ingest :as ingest]))

(deftest too-long
  (testing "too-long? should accept normal length Events"
           (is (not (ingest/too-long? "{\"status\":\"ok\",\"message-type\":\"event\",\"message\":{\"event\":{\"license\":\"https://creativecommons.org/publicdomain/zero/1.0/\",\"obj_id\":\"https://doi.org/10.1080/026999300402763\",\"source_token\":\"36c35e23-8757-4a9d-aacf-345e9b7eb50d\",\"occurred_at\":\"2018-11-26T12:50:02Z\",\"subj_id\":\"https://en.wikibooks.org/api/rest_v1/page/html/Evidence_in_driverless_cars/3490408\",\"id\":\"771566d9-7f52-4088-9767-163fff4bcabe\",\"evidence_record\":\"https://evidence.eventdata.crossref.org/evidence/20181126-wikipedia-97b835a8-d298-42bf-9dc2-78551234f4c2\",\"terms\":\"https://doi.org/10.13003/CED-terms-of-use\",\"action\":\"add\",\"subj\":{\"pid\":\"https://en.wikibooks.org/wiki/Evidence_in_driverless_cars\",\"url\":\"https://en.wikibooks.org/w/index.php?title=Evidence_in_driverless_cars&oldid=3490408\",\"title\":\"Evidence in driverless cars\",\"work_type_id\":\"entry-encyclopedia\",\"api-url\":\"https://en.wikibooks.org/api/rest_v1/page/html/Evidence_in_driverless_cars/3490408\"},\"source_id\":\"wikipedia\",\"obj\":{\"pid\":\"https://doi.org/10.1080/026999300402763\",\"url\":\"https://doi.org/10.1080/026999300402763\",\"method\":\"doi-literal\",\"verification\":\"literal\"},\"timestamp\":\"2018-11-26T14:24:03Z\",\"relation_type_id\":\"references\"}}}"))))


   (testing "too-long? should reject abnormally large Events"
            ; True story.
            (is (ingest/too-long? "{\"license\":\"https://creativecommons.org/publicdomain/zero/1.0/\",\"obj_id\":\"https://doi.org/10.17182/hepdata.83544.v1/t14\",\"source_token\":\"29a9a478-518f-4cbd-a133-a0dcef63d547\",\"occurred_at\":\"2019-03-18T22:15:50.000Z\",\"subj_id\":\"https://doi.org/10.17182/hepdata.83544.v1\",\"id\":\"56e57c42-5b47-4f0d-9782-0c8bbabcd1ed\",\"subj\":{\"publisher\":{\"@type\":\"Organization\",\"name\":\"HEPData\"},\"name\":\"Probing the quantum interference between singly and doubly resonant top-quark production in $pp$ collisions at $sqrt{s}=13$ TeV with the ATLAS detector\",\"datePublished\":\"2019\",\"registrantId\":\"datacite.cern.hepdata\",\"author\":[{\"name\":\"Morad Aaboud\",\"givenName\":\"Morad\",\"familyName\":\"Aaboud\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oujda U.\"},\"@type\":\"Person\"},{\"name\":\"Georges Aad\",\"givenName\":\"Georges\",\"familyName\":\"Aad\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Marseille, CPPM\"},\"@type\":\"Person\"},{\"name\":\"Brad Abbott\",\"givenName\":\"Brad\",\"familyName\":\"Abbott\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oklahoma U.\"},\"@type\":\"Person\"},{\"name\":\"Ovsat Abdinov\",\"givenName\":\"Ovsat\",\"familyName\":\"Abdinov\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Baku, Inst. Phys.\"},\"@type\":\"Person\"},{\"name\":\"Baptiste Abeloos\",\"givenName\":\"Baptiste\",\"familyName\":\"Abeloos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Orsay, LAL\"},\"@type\":\"Person\"},{\"name\":\"Deshan Kavishka Abhayasinghe\",\"givenName\":\"Deshan Kavishka\",\"familyName\":\"Abhayasinghe\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Royal Holloway, U. of London\"},\"@type\":\"Person\"},{\"name\":\"Syed Haider Abidi\",\"givenName\":\"Syed Haider\",\"familyName\":\"Abidi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Toronto U.\"},\"@type\":\"Person\"},{\"name\":\"Ossama Abouzeid\",\"givenName\":\"Ossama\",\"familyName\":\"Abouzeid\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Bohr Inst.\"},\"@type\":\"Person\"},{\"name\":\"Nicola Abraham\",\"givenName\":\"Nicola\",\"familyName\":\"Abraham\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Sussex U.\"},\"@type\":\"Person\"},{\"name\":\"Halina Abramowicz\",\"givenName\":\"Halina\",\"familyName\":\"Abramowicz\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Tel Aviv U.\"},\"@type\":\"Person\"},{\"name\":\"Henso Abreu\",\"givenName\":\"Henso\",\"familyName\":\"Abreu\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Technion\"},\"@type\":\"Person\"},{\"name\":\"Yiming Abulaiti\",\"givenName\":\"Yiming\",\"familyName\":\"Abulaiti\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Argonne\"},\"@type\":\"Person\"},{\"name\":\"Bobby Samir Acharya\",\"givenName\":\"Bobby Samir\",\"familyName\":\"Acharya\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Udine\"},\"@type\":\"Person\"},{\"name\":\"Shunsuke Adachi\",\"givenName\":\"Shunsuke\",\"familyName\":\"Adachi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Tokyo U., ICEPP\"},\"@type\":\"Person\"},{\"name\":\"Leszek Adamczyk\",\"givenName\":\"Leszek\",\"familyName\":\"Adamczyk\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"AGH-UST, Cracow\"},\"@type\":\"Person\"},{\"name\":\"Jahred Adelman\",\"givenName\":\"Jahred\",\"familyName\":\"Adelman\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Northern Illinois U.\"},\"@type\":\"Person\"},{\"name\":\"Michael Adersberger\",\"givenName\":\"Michael\",\"familyName\":\"Adersberger\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Munich U.\"},\"@type\":\"Person\"},{\"name\":\"Aytul Adiguzel\",\"givenName\":\"Aytul\",\"familyName\":\"Adiguzel\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Bogazici U.\"},\"@type\":\"Person\"},{\"name\":\"Tim Adye\",\"givenName\":\"Tim\",\"familyName\":\"Adye\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Rutherford\"},\"@type\":\"Person\"},{\"name\":\"Tony Affolder\",\"givenName\":\"Tony\",\"familyName\":\"Affolder\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"UC, Santa Cruz\"},\"@type\":\"Person\"},{\"name\":\"Yoav Afik\",\"givenName\":\"Yoav\",\"familyName\":\"Afik\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Technion\"},\"@type\":\"Person\"},{\"name\":\"Catalin Agheorghiesei\",\"givenName\":\"Catalin\",\"familyName\":\"Agheorghiesei\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Cuza U.\"},\"@type\":\"Person\"},{\"name\":\"Juan Antonio Aguilar Saavedra\",\"givenName\":\"Juan Antonio\",\"familyName\":\"Aguilar Saavedra\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"U. Granada, Dept. Theor. Phys. Cosmos\"},\"@type\":\"Person\"},{\"name\":\"Faig Ahmadov\",\"givenName\":\"Faig\",\"familyName\":\"Ahmadov\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Dubna, JINR\"},\"@type\":\"Person\"},{\"name\":\"Giulio Aielli\",\"givenName\":\"Giulio\",\"familyName\":\"Aielli\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Rome2\"},\"@type\":\"Person\"},{\"name\":\"Shunichi Akatsuka\",\"givenName\":\"Shunichi\",\"familyName\":\"Akatsuka\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Kyoto U.\"},\"@type\":\"Person\"},{\"name\":\"Torsten Paul Ake Akesson\",\"givenName\":\"Torsten Paul Ake\",\"familyName\":\"Akesson\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Lund U.\"},\"@type\":\"Person\"},{\"name\":\"Ece Akilli\",\"givenName\":\"Ece\",\"familyName\":\"Akilli\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Geneva U.\"},\"@type\":\"Person\"},{\"name\":\"Andrei Akimov\",\"givenName\":\"Andrei\",\"familyName\":\"Akimov\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Lebedev Inst.\"},\"@type\":\"Person\"},{\"name\":\"Gian Luigi Alberghi\",\"givenName\":\"Gian Luigi\",\"familyName\":\"Alberghi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Bologna\"},\"@type\":\"Person\"},{\"name\":\"Justin Albert\",\"givenName\":\"Justin\",\"familyName\":\"Albert\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Victoria U.\"},\"@type\":\"Person\"},{\"name\":\"Pietro Albicocco\",\"givenName\":\"Pietro\",\"familyName\":\"Albicocco\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Frascati\"},\"@type\":\"Person\"},{\"name\":\"Maria Josefina Alconada Verzini\",\"givenName\":\"Maria Josefina\",\"familyName\":\"Alconada Verzini\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"La Plata U.\"},\"@type\":\"Person\"},{\"name\":\"Sara Caroline Alderweireldt\",\"givenName\":\"Sara Caroline\",\"familyName\":\"Alderweireldt\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Nijmegen U.\"},\"@type\":\"Person\"},{\"name\":\"Martin Aleksa\",\"givenName\":\"Martin\",\"familyName\":\"Aleksa\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"CERN\"},\"@type\":\"Person\"},{\"name\":\"Igor Aleksandrov\",\"givenName\":\"Igor\",\"familyName\":\"Aleksandrov\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Dubna, JINR\"},\"@type\":\"Person\"},{\"name\":\"Calin Alexa\",\"givenName\":\"Calin\",\"familyName\":\"Alexa\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Bucharest, IFIN-HH\"},\"@type\":\"Person\"},{\"name\":\"Theodoros Alexopoulos\",\"givenName\":\"Theodoros\",\"familyName\":\"Alexopoulos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Natl. Tech. U., Athens\"},\"@type\":\"Person\"},{\"name\":\"Muhammad Alhroob\",\"givenName\":\"Muhammad\",\"familyName\":\"Alhroob\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oklahoma U.\"},\"@type\":\"Person\"},{\"name\":\"Babar Ali\",\"givenName\":\"Babar\",\"familyName\":\"Ali\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Prague, Tech. U.\"},\"@type\":\"Person\"},{\"name\":\"Gianluca Alimonti\",\"givenName\":\"Gianluca\",\"familyName\":\"Alimonti\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Milan\"},\"@type\":\"Person\"},{\"name\":\"John Alison\",\"givenName\":\"John\",\"familyName\":\"Alison\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Chicago U., EFI\"},\"@type\":\"Person\"},{\"name\":\"Steven Patrick Alkire\",\"givenName\":\"Steven Patrick\",\"familyName\":\"Alkire\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Washington U., Seattle\"},\"@type\":\"Person\"},{\"name\":\"Corentin Allaire\",\"givenName\":\"Corentin\",\"familyName\":\"Allaire\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Orsay, LAL\"},\"@type\":\"Person\"},{\"name\":\"Benedict Allbrooke\",\"givenName\":\"Benedict\",\"familyName\":\"Allbrooke\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Sussex U.\"},\"@type\":\"Person\"},{\"name\":\"Benjamin William Allen\",\"givenName\":\"Benjamin William\",\"familyName\":\"Allen\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oregon U.\"},\"@type\":\"Person\"},{\"name\":\"Phillip Allport\",\"givenName\":\"Phillip\",\"familyName\":\"Allport\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Birmingham U.\"},\"@type\":\"Person\"},{\"name\":\"Alberto Aloisio\",\"givenName\":\"Alberto\",\"familyName\":\"Aloisio\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Naples\"},\"@type\":\"Person\"},{\"name\":\"Alejandro Alonso\",\"givenName\":\"Alejandro\",\"familyName\":\"Alonso\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Bohr Inst.\"},\"@type\":\"Person\"},{\"name\":\"Francisco Alonso\",\"givenName\":\"Francisco\",\"familyName\":\"Alonso\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"La Plata U.\"},\"@type\":\"Person\"},{\"name\":\"Cristiano Alpigiani\",\"givenName\":\"Cristiano\",\"familyName\":\"Alpigiani\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Washington U., Seattle\"},\"@type\":\"Person\"},{\"name\":\"Azzah Aziz Alshehri\",\"givenName\":\"Azzah Aziz\",\"familyName\":\"Alshehri\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Glasgow U.\"},\"@type\":\"Person\"},{\"name\":\"Mahmoud Alstaty\",\"givenName\":\"Mahmoud\",\"familyName\":\"Alstaty\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Marseille, CPPM\"},\"@type\":\"Person\"},{\"name\":\"Barbara Alvarez Gonzalez\",\"givenName\":\"Barbara\",\"familyName\":\"Alvarez Gonzalez\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"CERN\"},\"@type\":\"Person\"},{\"name\":\"Damian Alvarez Piqueras\",\"givenName\":\"Damian\",\"familyName\":\"Alvarez Piqueras\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Valencia U., IFIC\"},\"@type\":\"Person\"},{\"name\":\"Mariagrazia Alviggi\",\"givenName\":\"Mariagrazia\",\"familyName\":\"Alviggi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Naples\"},\"@type\":\"Person\"},{\"name\":\"Brian Thomas Amadio\",\"givenName\":\"Brian Thomas\",\"familyName\":\"Amadio\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"LBL, Berkeley\"},\"@type\":\"Person\"},{\"name\":\"Yara Amaral Coutinho\",\"givenName\":\"Yara\",\"familyName\":\"Amaral Coutinho\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Rio de Janeiro Federal U.\"},\"@type\":\"Person\"},{\"name\":\"Luca Ambroz\",\"givenName\":\"Luca\",\"familyName\":\"Ambroz\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oxford U.\"},\"@type\":\"Person\"},{\"name\":\"Christoph Amelung\",\"givenName\":\"Christoph\",\"familyName\":\"Amelung\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Brandeis U.\"},\"@type\":\"Person\"},{\"name\":\"Dante Eric Amidei\",\"givenName\":\"Dante Eric\",\"familyName\":\"Amidei\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Michigan U.\"},\"@type\":\"Person\"},{\"name\":\"Susana Patricia Amor Dos Santos\",\"givenName\":\"Susana Patricia\",\"familyName\":\"Amor Dos Santos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"LIP, Lisbon\"},\"@type\":\"Person\"},{\"name\":\"Simone Amoroso\",\"givenName\":\"Simone\",\"familyName\":\"Amoroso\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"DESY\"},\"@type\":\"Person\"},{\"name\":\"Cherifa Sabrina Amrouche\",\"givenName\":\"Cherifa Sabrina\",\"familyName\":\"Amrouche\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Geneva U.\"},\"@type\":\"Person\"},{\"name\":\"Christos Anastopoulos\",\"givenName\":\"Christos\",\"familyName\":\"Anastopoulos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Sheffield U.\"},\"@type\":\"Person\"},{\"name\":\"Lucian Stefan Ancu\",\"givenName\":\"Lucian Stefan\",\"familyName\":\"Ancu\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Geneva U.\"},\"@type\":\"Person\"},{\"name\":\"Nansi Andari\",\"givenName\":\"Nansi\",\"familyName\":\"Andari\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Birmingham U.\"},\"@type\":\"Person\"},{\"name\":\"Timothy Andeen\",\"givenName\":\"Timothy\",\"familyName\":\"Andeen\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Texas U.\"},\"@type\":\"Person\"},{\"name\":\"Christoph Falk Anders\",\"givenName\":\"Christoph Falk\",\"familyName\":\"Anders\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Heidelberg U.\"},\"@type\":\"Person\"},{\"name\":\"John Kenneth Anders\",\"givenName\":\"John Kenneth\",\"familyName\":\"Anders\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Bern U., LHEP\"},\"@type\":\"Person\"},{\"name\":\"Kelby Anderson\",\"givenName\":\"Kelby\",\"familyName\":\"Anderson\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Chicago U., EFI\"},\"@type\":\"Person\"},{\"name\":\"Attilio Andreazza\",\"givenName\":\"Attilio\",\"familyName\":\"Andreazza\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Milan\"},\"@type\":\"Person\"},{\"name\":\"George Victor Andrei\",\"givenName\":\"George Victor\",\"familyName\":\"Andrei\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Kirchhoff Inst. Phys.\"},\"@type\":\"Person\"},{\"name\":\"Christopher Ryan Anelli\",\"givenName\":\"Christopher Ryan\",\"familyName\":\"Anelli\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Victoria U.\"},\"@type\":\"Person\"},{\"name\":\"Stylianos Angelidakis\",\"givenName\":\"Stylianos\",\"familyName\":\"Angelidakis\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Clermont-Ferrand U.\"},\"@type\":\"Person\"},{\"name\":\"Ivan Angelozzi\",\"givenName\":\"Ivan\",\"familyName\":\"Angelozzi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"FOM, Amsterdam\"},\"@type\":\"Person\"},{\"name\":\"Aaron Angerami\",\"givenName\":\"Aaron\",\"familyName\":\"Angerami\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Nevis Labs, Columbia U.\"},\"@type\":\"Person\"},{\"name\":\"Alexey Anisenkov\",\"givenName\":\"Alexey\",\"familyName\":\"Anisenkov\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Novosibirsk State U.\"},\"@type\":\"Person\"},{\"name\":\"Alberto Annovi\",\"givenName\":\"Alberto\",\"familyName\":\"Annovi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Pisa\"},\"@type\":\"Person\"},{\"name\":\"Claire Antel\",\"givenName\":\"Claire\",\"familyName\":\"Antel\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Kirchhoff Inst. Phys.\"},\"@type\":\"Person\"},{\"name\":\"Matthew Thomas Anthony\",\"givenName\":\"Matthew Thomas\",\"familyName\":\"Anthony\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Sheffield U.\"},\"@type\":\"Person\"},{\"name\":\"Mario Antonelli\",\"givenName\":\"Mario\",\"familyName\":\"Antonelli\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Frascati\"},\"@type\":\"Person\"},{\"name\":\"Daniel Joseph Antrim\",\"givenName\":\"Daniel Joseph\",\"familyName\":\"Antrim\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"UC, Irvine\"},\"@type\":\"Person\"},{\"name\":\"Fabio Anulli\",\"givenName\":\"Fabio\",\"familyName\":\"Anulli\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Rome\"},\"@type\":\"Person\"},{\"name\":\"Masato Aoki\",\"givenName\":\"Masato\",\"familyName\":\"Aoki\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"KEK, Tsukuba\"},\"@type\":\"Person\"},{\"name\":\"Ludovica Aperio Bella\",\"givenName\":\"Ludovica\",\"familyName\":\"Aperio Bella\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"CERN\"},\"@type\":\"Person\"},{\"name\":\"Giorgi Arabidze\",\"givenName\":\"Giorgi\",\"familyName\":\"Arabidze\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Michigan State U.\"},\"@type\":\"Person\"},{\"name\":\"Juan Pedro Araque Espinosa\",\"givenName\":\"Juan Pedro\",\"familyName\":\"Araque Espinosa\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"LIP, Lisbon\"},\"@type\":\"Person\"},{\"name\":\"Victor Araujo Ferraz\",\"givenName\":\"Victor\",\"familyName\":\"Araujo Ferraz\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Rio de Janeiro Federal U.\"},\"@type\":\"Person\"},{\"name\":\"Rodrigo Araujo Pereira\",\"givenName\":\"Rodrigo\",\"familyName\":\"Araujo Pereira\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Rio de Janeiro Federal U.\"},\"@type\":\"Person\"},{\"name\":\"Ayana Arce\",\"givenName\":\"Ayana\",\"familyName\":\"Arce\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Duke U.\"},\"@type\":\"Person\"},{\"name\":\"Rose Elisabeth Ardell\",\"givenName\":\"Rose Elisabeth\",\"familyName\":\"Ardell\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Royal Holloway, U. of London\"},\"@type\":\"Person\"},{\"name\":\"Francisco Anuar Arduh\",\"givenName\":\"Francisco Anuar\",\"familyName\":\"Arduh\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"La Plata U.\"},\"@type\":\"Person\"},{\"name\":\"Jean-Francois Arguin\",\"givenName\":\"Jean-Francois\",\"familyName\":\"Arguin\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Montreal U.\"},\"@type\":\"Person\"},{\"name\":\"Spyridon Argyropoulos\",\"givenName\":\"Spyridon\",\"familyName\":\"Argyropoulos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Iowa U.\"},\"@type\":\"Person\"},{\"name\":\"Aaron James Armbruster\",\"givenName\":\"Aaron James\",\"familyName\":\"Armbruster\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"CERN\"},\"@type\":\"Person\"},{\"name\":\"Lewis James Armitage\",\"givenName\":\"Lewis James\",\"familyName\":\"Armitage\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Queen Mary, U. of London\"},\"@type\":\"Person\"},{\"name\":\"Alexander Armstrong\",\"givenName\":\"Alexander\",\"familyName\":\"Armstrong\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"UC, Irvine\"},\"@type\":\"Person\"},{\"name\":\"Olivier Arnaez\",\"givenName\":\"Olivier\",\"familyName\":\"Arnaez\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Toronto U.\"},\"@type\":\"Person\"},{\"name\":\"Hannah Arnold\",\"givenName\":\"Hannah\",\"familyName\":\"Arnold\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"FOM, Amsterdam\"},\"@type\":\"Person\"},{\"name\":\"Miguel Arratia\",\"givenName\":\"Miguel\",\"familyName\":\"Arratia\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Cambridge U.\"},\"@type\":\"Person\"},{\"name\":\"Ozan Arslan\",\"givenName\":\"Ozan\",\"familyName\":\"Arslan\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Bonn U.\"},\"@type\":\"Person\"},{\"name\":\"Andrei Artamonov\",\"givenName\":\"Andrei\",\"familyName\":\"Artamonov\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Moscow, ITEP\"},\"@type\":\"Person\"},{\"name\":\"Giacomo Artoni\",\"givenName\":\"Giacomo\",\"familyName\":\"Artoni\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oxford U.\"},\"@type\":\"Person\"},{\"name\":\"Sebastian Artz\",\"givenName\":\"Sebastian\",\"familyName\":\"Artz\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Mainz U.\"},\"@type\":\"Person\"},{\"name\":\"Shoji Asai\",\"givenName\":\"Shoji\",\"familyName\":\"Asai\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Tokyo U., ICEPP\"},\"@type\":\"Person\"},{\"name\":\"Nedaa Asbah\",\"givenName\":\"Nedaa\",\"familyName\":\"Asbah\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"DESY\"},\"@type\":\"Person\"},{\"name\":\"Adi Ashkenazi\",\"givenName\":\"Adi\",\"familyName\":\"Ashkenazi\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Tel Aviv U.\"},\"@type\":\"Person\"},{\"name\":\"Eleni Myrto Asimakopoulou\",\"givenName\":\"Eleni Myrto\",\"familyName\":\"Asimakopoulou\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Uppsala U., Inst. Theor. Phys.\"},\"@type\":\"Person\"},{\"name\":\"Lily Asquith\",\"givenName\":\"Lily\",\"familyName\":\"Asquith\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Sussex U.\"},\"@type\":\"Person\"},{\"name\":\"Ketevi Assamagan\",\"givenName\":\"Ketevi\",\"familyName\":\"Assamagan\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Brookhaven\"},\"@type\":\"Person\"},{\"name\":\"Robert Astalos\",\"givenName\":\"Robert\",\"familyName\":\"Astalos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Comenius U.\"},\"@type\":\"Person\"},{\"name\":\"Ryan Justin Atkin\",\"givenName\":\"Ryan Justin\",\"familyName\":\"Atkin\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Cape Town U.\"},\"@type\":\"Person\"},{\"name\":\"Markus Atkinson\",\"givenName\":\"Markus\",\"familyName\":\"Atkinson\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Illinois U., Urbana\"},\"@type\":\"Person\"},{\"name\":\"Naim Bora Atlay\",\"givenName\":\"Naim Bora\",\"familyName\":\"Atlay\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Siegen U.\"},\"@type\":\"Person\"},{\"name\":\"Kamil Augsten\",\"givenName\":\"Kamil\",\"familyName\":\"Augsten\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Prague, Tech. U.\"},\"@type\":\"Person\"},{\"name\":\"Giuseppe Avolio\",\"givenName\":\"Giuseppe\",\"familyName\":\"Avolio\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"CERN\"},\"@type\":\"Person\"},{\"name\":\"Rachel Maria Avramidou\",\"givenName\":\"Rachel Maria\",\"familyName\":\"Avramidou\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Hefei, CUST\"},\"@type\":\"Person\"},{\"name\":\"Mohamad Kassem Ayoub\",\"givenName\":\"Mohamad Kassem\",\"familyName\":\"Ayoub\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Beijing, Inst. High Energy Phys.\"},\"@type\":\"Person\"},{\"name\":\"Georges Azuelos\",\"givenName\":\"Georges\",\"familyName\":\"Azuelos\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Montreal U.\"},\"@type\":\"Person\"},{\"name\":\"Alessandra Baas\",\"givenName\":\"Alessandra\",\"familyName\":\"Baas\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Kirchhoff Inst. Phys.\"},\"@type\":\"Person\"},{\"name\":\"Matthew John Baca\",\"givenName\":\"Matthew John\",\"familyName\":\"Baca\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Birmingham U.\"},\"@type\":\"Person\"},{\"name\":\"Henri Bachacou\",\"givenName\":\"Henri\",\"familyName\":\"Bachacou\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"IRFU, Saclay\"},\"@type\":\"Person\"},{\"name\":\"Konstantinos Bachas\",\"givenName\":\"Konstantinos\",\"familyName\":\"Bachas\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Lecce\"},\"@type\":\"Person\"},{\"name\":\"Moritz Backes\",\"givenName\":\"Moritz\",\"familyName\":\"Backes\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"Oxford U.\"},\"@type\":\"Person\"},{\"name\":\"Paolo Bagnaia\",\"givenName\":\"Paolo\",\"familyName\":\"Bagnaia\",\"affiliation\":{\"@type\":\"Organization\",\"name\":\"INFN, Rome\"},\"@type\":\"Person\"}],\"dateModified\":\"2019-03-18T22:15:50.000Z\",\"@id\":\"https://doi.org/10.17182/hepdata.83544.v1\",\"proxyIdentifiers\":[\"n                http://www.inspirehep.net/record/1677498n            \",\"n                10.1103/physrevlett.121.152002n            \",\"n                http://www.inspirehep.net/record/1677498n            \",\"n                10.1103/physrevlett.121.152002n            \"],\"version\":\"1\",\"@type\":\"Collection\"},\"message_action\":\"create\",\"source_id\":\"datacite\",\"obj\":{},\"timestamp\":\"2019-03-18T22:18:30Z\",\"relation_type_id\":\"is_supplemented_by\"}"))))