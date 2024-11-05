import {gql} from 'graphql-request';

export const autocompleteSchoolQuery = gql`
query AutocompleteSearchQuery(
  $query: String!
)
{
  autocomplete(query: $query) {
    schools {
      edges {
        node {
          id
          name
          city
          state
        }
      }
    }
  }
}`;

export const autocompleteTeacherQuery = gql`
query AutocompleteSearchQuery(
  $query: String!
)
{
  autocomplete(query: $query) {
    teachers {
      edges {
        node {
          id
          firstName
          lastName
          school {
            name
            id
          }
        }
      }
    }
  }
}`;

export const searchTeacherQuery = gql`
query NewSearchTeachersQuery($text: String!, $schoolID: ID!)
{
  newSearch {
    teachers(query: {text: $text, schoolID: $schoolID}) {
      edges {
        cursor
        node {
          id
          firstName
          lastName
          school {
            name
            id
          }
        }
      }
    }
  }
}
`;

export const getTeacherQuery = gql`
query TeacherRatingsPageQuery(
  $id: ID!
) {
  node(id: $id) {
    ... on Teacher {
      id
      firstName
      lastName
      school {
        name
        id
        city
        state
      }
      avgDifficulty
      avgRating
      courseCodes {
        courseName
        courseCount
      }
      department
      numRatings
      legacyId
      wouldTakeAgainPercent
      numRatings
      ratings(first: 1000) {
        edges {
          node {
            class
            comment
            date
            difficultyRating
            
          }
        }
      }
    }
    id
  }
}
`;


// new features below
export const getTeacherRatingsPageQuery = gql`
query TeacherRatingsPageQuery(
  $id: ID!
) {
  node(id: $id) {
    __typename
    ... on Teacher {
      id
      legacyId
      firstName
      lastName
      department
      school {
        legacyId
        name
        city
        state
        country
        id
      }
      lockStatus
      ...StickyHeader_teacher
      ...RatingDistributionWrapper_teacher
      ...TeacherInfo_teacher
      ...SimilarProfessors_teacher
      ...TeacherRatingTabs_teacher
    }
    id
  }
}

fragment StickyHeader_teacher on Teacher {
  ...HeaderDescription_teacher
  ...HeaderRateButton_teacher
}

fragment RatingDistributionWrapper_teacher on Teacher {
  ...NoRatingsArea_teacher
  ratingsDistribution {
    total
    ...RatingDistributionChart_ratingsDistribution
  }
}

fragment TeacherInfo_teacher on Teacher {
  id
  lastName
  numRatings
  ...RatingValue_teacher
  ...NameTitle_teacher
  ...TeacherTags_teacher
  ...NameLink_teacher
  ...TeacherFeedback_teacher
  ...RateTeacherLink_teacher
}

fragment SimilarProfessors_teacher on Teacher {
  department
  relatedTeachers {
    legacyId
    ...SimilarProfessorListItem_teacher
    id
  }
}

fragment TeacherRatingTabs_teacher on Teacher {
  numRatings
  courseCodes {
    courseName
    courseCount
  }
  ...RatingsList_teacher
  ...RatingsFilter_teacher
}

fragment RatingsList_teacher on Teacher {
  id
  legacyId
  lastName
  numRatings
  school {
    id
    legacyId
    name
    city
    state
    avgRating
    numRatings
  }
  ...Rating_teacher
  ...NoRatingsArea_teacher
  ratings(first: 1000) {
    edges {
      cursor
      node {
        ...Rating_rating
        id
        __typename
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}

fragment RatingsFilter_teacher on Teacher {
  courseCodes {
    courseCount
    courseName
  }
}

fragment Rating_teacher on Teacher {
  ...RatingFooter_teacher
  ...RatingSuperHeader_teacher
  ...ProfessorNoteSection_teacher
}

fragment NoRatingsArea_teacher on Teacher {
  lastName
  ...RateTeacherLink_teacher
}

fragment Rating_rating on Rating {
  comment
  flagStatus
  createdByUser
  teacherNote {
    id
  }
  ...RatingHeader_rating
  ...RatingSuperHeader_rating
  ...RatingValues_rating
  ...CourseMeta_rating
  ...RatingTags_rating
  ...RatingFooter_rating
  ...ProfessorNoteSection_rating
}

fragment RatingHeader_rating on Rating {
  date
  class
  helpfulRating
  clarityRating
  isForOnlineClass
}

fragment RatingSuperHeader_rating on Rating {
  legacyId
}

fragment RatingValues_rating on Rating {
  helpfulRating
  clarityRating
  difficultyRating
}

fragment CourseMeta_rating on Rating {
  attendanceMandatory
  wouldTakeAgain
  grade
  textbookUse
  isForOnlineClass
  isForCredit
}

fragment RatingTags_rating on Rating {
  ratingTags
}

fragment RatingFooter_rating on Rating {
  id
  comment
  adminReviewedAt
  flagStatus
  legacyId
  thumbsUpTotal
  thumbsDownTotal
  thumbs {
    thumbsUp
    thumbsDown
    computerId
    id
  }
  teacherNote {
    id
  }
}

fragment ProfessorNoteSection_rating on Rating {
  teacherNote {
    ...ProfessorNote_note
    id
  }
  ...ProfessorNoteEditor_rating
}

fragment ProfessorNote_note on TeacherNotes {
  comment
  ...ProfessorNoteHeader_note
  ...ProfessorNoteFooter_note
}

fragment ProfessorNoteEditor_rating on Rating {
  id
  legacyId
  class
  teacherNote {
    id
    teacherId
    comment
  }
}

fragment ProfessorNoteHeader_note on TeacherNotes {
  createdAt
  updatedAt
}

fragment ProfessorNoteFooter_note on TeacherNotes {
  legacyId
  flagStatus
}

fragment RateTeacherLink_teacher on Teacher {
  legacyId
  numRatings
  lockStatus
}

fragment RatingFooter_teacher on Teacher {
  id
  legacyId
  lockStatus
  isProfCurrentUser
}

fragment RatingSuperHeader_teacher on Teacher {
  firstName
  lastName
  legacyId
  school {
    name
    id
  }
}

fragment ProfessorNoteSection_teacher on Teacher {
  ...ProfessorNote_teacher
  ...ProfessorNoteEditor_teacher
}

fragment ProfessorNote_teacher on Teacher {
  ...ProfessorNoteHeader_teacher
  ...ProfessorNoteFooter_teacher
}

fragment ProfessorNoteEditor_teacher on Teacher {
  id
}

fragment ProfessorNoteHeader_teacher on Teacher {
  lastName
}

fragment ProfessorNoteFooter_teacher on Teacher {
  legacyId
  isProfCurrentUser
}

fragment SimilarProfessorListItem_teacher on RelatedTeacher {
  legacyId
  firstName
  lastName
  avgRating
}

fragment RatingValue_teacher on Teacher {
  avgRating
  numRatings
  ...NumRatingsLink_teacher
}

fragment NameTitle_teacher on Teacher {
  id
  firstName
  lastName
  department
  school {
    legacyId
    name
    id
  }
  ...TeacherDepartment_teacher
  ...TeacherBookmark_teacher
}

fragment TeacherTags_teacher on Teacher {
  lastName
  teacherRatingTags {
    legacyId
    tagCount
    tagName
    id
  }
}

fragment NameLink_teacher on Teacher {
  isProfCurrentUser
  id
  legacyId
  firstName
  lastName
  school {
    name
    id
  }
}

fragment TeacherFeedback_teacher on Teacher {
  numRatings
  avgDifficulty
  wouldTakeAgainPercent
}

fragment TeacherDepartment_teacher on Teacher {
  department
  departmentId
  school {
    legacyId
    name
    id
  }
}

fragment TeacherBookmark_teacher on Teacher {
  id
  isSaved
}

fragment NumRatingsLink_teacher on Teacher {
  numRatings
  ...RateTeacherLink_teacher
}

fragment RatingDistributionChart_ratingsDistribution on ratingsDistribution {
  r1
  r2
  r3
  r4
  r5
}

fragment HeaderDescription_teacher on Teacher {
  id
  firstName
  lastName
  department
  school {
    legacyId
    name
    city
    state
    id
  }
  ...TeacherTitles_teacher
  ...TeacherBookmark_teacher
}

fragment HeaderRateButton_teacher on Teacher {
  ...RateTeacherLink_teacher
}

fragment TeacherTitles_teacher on Teacher {
  department
  school {
    legacyId
    name
    id
  }
}
`;


export const getDepartmentPaginationQuery = gql`
query TeacherSearchPaginationQuery(
  $count: Int!
  $cursor: String
  $query: TeacherSearchQuery!
) {
  search: newSearch {
    ...TeacherSearchPagination_search_1jWD3d
  }
}

fragment TeacherSearchPagination_search_1jWD3d on newSearch {
  teachers(query: $query, first: $count, after: $cursor) {
    didFallback
    edges {
      cursor
      node {
        ...TeacherCard_teacher
        id
        __typename
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    resultCount
    filters {
      field
      options {
        value
        id
      }
    }
  }
}

fragment TeacherCard_teacher on Teacher {
  id
  legacyId
  avgRating
  numRatings
  ...CardFeedback_teacher
  ...CardSchool_teacher
  ...CardName_teacher
  ...TeacherBookmark_teacher
}

fragment CardFeedback_teacher on Teacher {
  wouldTakeAgainPercent
  avgDifficulty
}

fragment CardSchool_teacher on Teacher {
  department
  school {
    name
    id
  }
}

fragment CardName_teacher on Teacher {
  firstName
  lastName
}

fragment TeacherBookmark_teacher on Teacher {
  id
  isSaved
}
`;


export const getDepartmentFirstPageQuery = gql`
query TeacherSearchResultsPageQuery(
  $query: TeacherSearchQuery!
  $schoolID: ID
) {
  search: newSearch {
    ...TeacherSearchPagination_search_1ZLmLD
  }
  school: node(id: $schoolID) {
    __typename
    ... on School {
      name
    }
    id
  }
}

fragment TeacherSearchPagination_search_1ZLmLD on newSearch {
  teachers(query: $query, first: 1000, after: "") {
    didFallback
    edges {
      cursor
      node {
        ...TeacherCard_teacher
        id
        __typename
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    resultCount
    filters {
      field
      options {
        value
        id
      }
    }
  }
}

fragment TeacherCard_teacher on Teacher {
  id
  legacyId
  avgRating
  numRatings
  ...CardFeedback_teacher
  ...CardSchool_teacher
  ...CardName_teacher
  ...TeacherBookmark_teacher
}

fragment CardFeedback_teacher on Teacher {
  wouldTakeAgainPercent
  avgDifficulty
}

fragment CardSchool_teacher on Teacher {
  department
  school {
    name
    id
  }
}

fragment CardName_teacher on Teacher {
  firstName
  lastName
}

fragment TeacherBookmark_teacher on Teacher {
  id
  isSaved
}
`;