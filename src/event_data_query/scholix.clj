(ns event-data-query.scholix
  "Transform Events into Scholix format."
  (:require [crossref.util.doi :as cr-doi]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [camel-snake-kebab.core :refer :all]))

(def scholix-relations 
{; Inputs taken from:
 ; http://schema.datacite.org/meta/kernel-4.1/include/datacite-relationType-v4.1.xsd
 ; http://data.crossref.org/schemas/relations.xsd
 "based_on_data"          "References"
 "cites"                  "References"
 "compiles"               "IsRelatedTo"
 "continues"              "References"
 "describes"              "IsRelatedTo"
 "documents"              "References"
 "has_comment"            "IsReferencedBy"
 "has_derivation"         "IsReferencedBy"
 "has_expression"         "IsRelatedTo"
 "has_format"             "IsRelatedTo"
 "has_manifestation"      "IsRelatedTo"
 "has_manuscript"         "IsSupplementedBy"
 "has_metadata"           "IsSupplementedBy"
 "has_part"               "IsRelatedTo"
 "has_preprint"           "IsReferencedBy"
 "has_related_material"   "IsSupplementedBy"
 "has_reply"              "IsReferencedBy"
 "has_review"             "IsReferencedBy"
 "has_translation"        "IsRelatedTo"
 "has_version"            "IsRelatedTo"
 "is_based_on"            "References"
 "is_basis_for"           "IsReferencedBy"
 "is_cited_by"            "IsReferencedBy"
 "is_comment_on"          "References"
 "is_compiled_by"         "IsRelatedTo"
 "is_continued_by"        "IsReferencedBy"
 "is_data_basis_for"      "IsReferencedBy"
 "is_derived_from"        "References"
 "is_described_by"        "IsSupplementedBy"
 "is_documented_by"       "IsSupplementedBy"
 "is_expression_of"       "IsRelatedTo"
 "is_format_of"           "IsRelatedTo"
 "is_identical_to"        "IsRelatedTo"
 "is_manifestation_of"    "IsRelatedTo"
 "is_manuscript_of"       "IsRelatedTo"
 "is_metadata_for"        "IsSupplementTo"
 "is_new_version_of"      "References"
 "is_original_form_of"    "IsReferencedBy"
 "is_part_of"             "IsRelatedTo"
 "is_preprint_of"         "IsRelatedTo"
 "is_previous_version_of" "IsRelatedTo"
 "is_referenced_by"       "IsReferencedBy"
 "is_related_material"    "IsSupplementTo"
 "is_replaced_by"         "IsReferencedBy"
 "is_reply_to"            "References"
 "is_required_by"         "IsRelatedTo"
 "is_review_of"           "References"
 "is_reviewed_by"         "IsReferencedBy"
 "is_same_as"             "IsRelatedTo"
 "is_source_of"           "IsReferencedBy"
 "is_supplement_to"       "IsSupplementTo"
 "is_supplemented_by"     "IsSupplementedBy"
 "is_translation_of"      "IsRelatedTo"
 "is_variant_form_of"     "IsRelatedTo"
 "is_version_of"          "IsRelatedTo"
 "references"             "References"
 "replaces"               "IsRelatedTo"
 "requires"               "IsRelatedTo"
 "reviews"                "References"})

(def scholix-content-types
  ; Inputs taken from:
  ; http://schema.datacite.org/meta/kernel-4.1/include/datacite-resourceType-v4.1.xsd
  ; https://github.com/CrossRef/cayenne/blob/master/src/cayenne/ids/type.clj
  {"audiovisual" "other"
   "book" "literature"
   "book_chapter"         "literature"
   "book_part"            "literature"
   "book_section"         "literature"
   "book_series"          "literature"
   "book_set"             "literature"
   "book_track"           "literature"
   "collection"           "other"
   "component"            "literature"
   "datapaper"            "literature"
   "dataset"              "dataset"
   "dissertation"         "literature"
   "edited_book"          "literature"
   "event"                "other"
   "image"                "other"
   "interactive_resource" "other"
   "journal"              "literature"
   "journal_article"      "literature"
   "journal_issue"        "literature"
   "journal_volume"       "literature"
   "model"                "other"
   "monograph"            "literature"
   "other"                "other"
   "physical_object"      "other"
   "posted_content"       "literature"
   "proceedings"          "literature"
   "proceedings_article"  "literature"
   "reference_book"       "literature"
   "reference_entry"      "literature"
   "report"               "literature"
   "report_series"        "literature"
   "service"              "other"
   "software"             "dataset"
   "sound"                "other"
   "standard"             "literature"
   "standard_series"      "literature"
   "text"                 "literature"
   "workflow"             "other"})

(defn ->scholix-relation
  [input]
  (-> input ->snake_case (scholix-relations "IsRelatedTo")))

(defn ->scholix-content-type
  [input]
  (-> input ->snake_case (scholix-content-types "Other")))

(def canonical-scholix-endpoint
  "https://api.eventdata.crossref.org/v1/events/scholix/")

(defn document->event
  "Convert a Document as stored in Elastic to a Scholix Link Information Package."
  [document]
  (try
    (when document
      {:LinkPublicationDate (-> document :event :timestamp)
                       :LinkProvider [{:Name (:source document)}]
                       :RelationshipType {:Name (-> document :relation-type ->scholix-relation)}
                       :LicenseURL (-> document :event :license)
                       :Url (str canonical-scholix-endpoint (:id document))
                       :Source {:Identifier {:ID (-> document :subj-doi cr-doi/non-url-doi)
                                                              :IDScheme "DOI" :IDUrl (-> document :subj-doi cr-doi/normalise-doi)}
                                :Type {:Name (-> document :subj-content-type ->scholix-content-type)
                                       :SubType (-> document :subj-content-type)
                                       ; TODO the schema here is just "crossref" or "datacite". Maybe we want to be more specific?
                                       :SubTypeSchema (:subj-ra document)}}

                       :Target {:Identifier {:ID (-> document :obj-doi cr-doi/non-url-doi)
                                                              :IDScheme "DOI" :IDUrl (-> document :obj-doi cr-doi/normalise-doi)}
                                :Type {:Name (-> document :obj-content-type ->scholix-content-type)
                                       :SubType (-> document :obj-content-type)
                                       ; Ditto Source.SubTypeSchema.
                                       :SubTypeSchema (:obj-ra document)}}})
    ; All the fields should be in place. However, since the content type and RA are looked up from an external source,
    ; data might be missing. There may be an NPE when this happens. Log and return nil. 
    ; NullPointerException, java.lang.IllegalArgumentException observed.
    (catch Exception ex
      (do
        (log/error "Failed to transform event" (:id document))
        (clojure.pprint/pprint document)
        nil))))
