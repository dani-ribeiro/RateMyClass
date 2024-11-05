import {GraphQLClient} from 'graphql-request';
import {autocompleteSchoolQuery, searchTeacherQuery, getTeacherRatingsPageQuery, getDepartmentPaginationQuery, getDepartmentFirstPageQuery } from './queries';
import {AUTH_TOKEN} from './constants';

const client = new GraphQLClient('https://www.ratemyprofessors.com/graphql', {
  headers: {
    authorization: `Basic ${AUTH_TOKEN}`, 'Access-Control-Allow-Origin': '*'
  }
});

export interface ISchoolFromSearch {
  id: string;
  name: string;
  city: string;
  state: string;
}

export interface ITeacherFromSearch {
  id: string;
  firstName: string;
  lastName: string;
  school: {
    id: string;
    name: string;
  };
}

export interface ITeacherPage {
  id: string;
  firstName: string;
  lastName: string;
  avgDifficulty: number;
  avgRating: number;
  numRatings: number;
  department: string;
  school: ISchoolFromSearch;
  legacyId: number;
}

export interface DepartmentQuery  {
  query: {
    text: string,
    schoolID: string,
    fallback: boolean,
    departmentID: string
  },
  schoolID: string
}

const searchSchool = async (query: string): Promise<ISchoolFromSearch[]> => {
  const response = await client.request(autocompleteSchoolQuery, {query});

  return response.autocomplete.schools.edges.map((edge: { node: ISchoolFromSearch }) => edge.node);
};

const searchTeacher = async (name: string, schoolID: string): Promise<ITeacherFromSearch[]> => {
  const response = await client.request(searchTeacherQuery, {
    text: name,
    schoolID
  });

  if (response.newSearch.teachers === null) {
    return [];
  }

  return response.newSearch.teachers.edges.map((edge: { node: ITeacherFromSearch }) => edge.node);
};

// old version
// const getTeacher = async (id: string): Promise<ITeacherPage> => {
//   const response = await client.request(getTeacherQuery, {id});

//   return response.node;
// };

const getTeacher = async (variables: Object): Promise<ITeacherPage> => {
  const response = await client.request(getTeacherRatingsPageQuery, variables);
  
  return response;
}

const getDepartmentPagination = async (variables: Object): Promise<ITeacherPage> => {
  const response = await client.request(getDepartmentPaginationQuery, variables);

  return response;
};


const getDepartmentFirstPage = async (variables: Object): Promise<ITeacherPage> => {
  const response = await client.request(getDepartmentFirstPageQuery, variables);

  return response;
}


const getAllProfessorsInDepartment = async (variables: DepartmentQuery): Promise<ITeacherPage> => {
  const response = await client.request(getDepartmentFirstPageQuery, variables);

  var hasNextPage = response.search.teachers.pageInfo.hasNextPage;
  while (hasNextPage === true) {
    const cursor = response.search.teachers.pageInfo.endCursor;
    const pagination_variables = {
      "count": 100,
      "cursor": cursor,
      "query": {
          "text": "",
          "schoolID": variables.schoolID,
          "fallback": true,
          "departmentID": variables.query.departmentID
      }
    }
    const newResponse = await client.request(getDepartmentPaginationQuery, pagination_variables);
    console.log(`Fetched ${newResponse.search.teachers.edges.length} new professors`);
    response.search.teachers.edges = [...response.search.teachers.edges, ...newResponse.search.teachers.edges];
    response.search.teachers.pageInfo = newResponse.search.teachers.pageInfo;
    hasNextPage = newResponse.search.teachers.pageInfo.hasNextPage;
    console.log(`Total professors fetched: ${response.search.teachers.edges.length}`);
  }

  return response;
}



export default {searchSchool, searchTeacher, getTeacher, getDepartmentPagination, getDepartmentFirstPage, getAllProfessorsInDepartment};
