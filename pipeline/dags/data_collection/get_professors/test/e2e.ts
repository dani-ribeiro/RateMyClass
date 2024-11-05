import test from 'ava';
import ratings from '../src';
import fs from 'fs';

const WASHU_ID = 'U2Nob29sLTExNDc=';

const WASHU_DEPARTMENT_IDS = {
  any: ''
};

const variables = {
  query: {
    text: '',
    schoolID: WASHU_ID,
    fallback: true,
    departmentID: WASHU_DEPARTMENT_IDS.any
  },
  schoolID: WASHU_ID
};

// Test to get all professors @ WashU
test('Get ALL Professors at WashU', async t => {
  const page = await ratings.getAllProfessorsInDepartment(variables);

  fs.writeFileSync('../get_reviews/professors.json', JSON.stringify(page, null, 2));

  console.log(page);
  t.pass();
});
