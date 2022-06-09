use regex::Regex;
use tokio::task::spawn_blocking;
use std::error::Error;
use graphql_client::{GraphQLQuery, Response, QueryBody};
use reqwest::{self, header};
use influxdb::{Client, Query, InfluxDbWriteable};
use chrono::{DateTime, Utc};
use futures::{executor, stream, Stream, TryStreamExt}; // 0.3.4
use tokio_stream::StreamExt;
use serde::{Deserialize, Serialize};

// to match the scalar of the same name
type JobID = String;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/gitlab_schema.graphql",
    query_path = "src/get_project_details.graphql",
    response_derives = "Debug",
)]
pub struct GetProjectDetailsQuery;

#[derive(Debug, Clone)]
pub struct ProjectDetailEntity{
    name: String,
    id: String,
    merge_requests: i64,
    test_job: JobCoverageEntity,
    pylint_job: JobScoreEntity,
    bandit_job: JobScoreEntity,
    safety_job: JobScoreEntity,
}

#[derive(Debug, Clone)]
pub struct JobCoverageEntity{
    id: String,
    name: String,
    coverage: f64
}

#[derive(Debug, Clone)]
pub struct JobScoreEntity{
    id: String,
    name: String,
    score: f64
}

#[derive(Serialize, Deserialize)]
#[derive(InfluxDbWriteable)]
pub struct FloatScoreModel{
    time: DateTime<Utc>,
    project_id: String,
    project_name: String,
    metric: String,
    score: f64,
}

fn extract_api_id_from_gid(gid: String) -> String{
    let re = Regex::new(r"gid://gitlab/([a-zA-Z0-9:]+)/(\d+)").unwrap();
    let cap = re.captures(&gid).unwrap();
    let score = &cap[2];
    String::from(score)
}

fn create_project_entity(project: get_project_details_query::GetProjectDetailsQueryUserProjectMembershipsNodesProject) -> ProjectDetailEntity{
    let pipeline_node = project.pipelines.unwrap().nodes.unwrap().into_iter().next().unwrap().unwrap();
    
    let test_job_pipeline = pipeline_node.test_job.unwrap();
    let test_job = JobCoverageEntity{
        id: extract_api_id_from_gid(test_job_pipeline.id.unwrap()), 
        coverage: test_job_pipeline.coverage.unwrap(),
        name: String::from(test_job_pipeline.name.unwrap())
    };    
    
    let pylint_job_pipeline = pipeline_node.pylint_job.unwrap();
    let pylint_job = JobScoreEntity{
        id: extract_api_id_from_gid(pylint_job_pipeline.id.unwrap()), 
        score: 0.0, 
        name: String::from(pylint_job_pipeline.name.unwrap())
    };    
    
    let bandit_job_pipeline = pipeline_node.bandit_job.unwrap();
    let bandit_job = JobScoreEntity{
        id: extract_api_id_from_gid(bandit_job_pipeline.id.unwrap()), 
        score: 0.0, 
        name: String::from(bandit_job_pipeline.name.unwrap())
    };    
    
    let safety_job_pipeline = pipeline_node.safety_job.unwrap();
    let safety_job = JobScoreEntity{
        id: extract_api_id_from_gid(safety_job_pipeline.id.unwrap()), 
        score: 0.0, 
        name: String::from(safety_job_pipeline.name.unwrap())
    };
    
    ProjectDetailEntity{
        id: extract_api_id_from_gid(project.id),
        name: project.name,
        merge_requests: project.merge_requests.unwrap().count,
        test_job,
        bandit_job,
        pylint_job,
        safety_job
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let project_names = spawn_blocking(move || {
        let user_projects: Response<get_project_details_query::ResponseData> = get_project_details_query("MSC291".to_string());
        let project_names: Vec<ProjectDetailEntity> = user_projects.data.unwrap().user.unwrap().project_memberships.unwrap().nodes.unwrap().into_iter().map(|x| create_project_entity(x.unwrap().project.unwrap())).collect::<Vec<ProjectDetailEntity>>();
        project_names
    }).await;
    
    let mut stream = tokio_stream::iter(project_names.unwrap());
    while let Some(mut project_detail) = stream.next().await {
        let pylint_project = project_detail.clone();
        let pylint_project_id = pylint_project.id;
        let pylint_job_id= pylint_project.pylint_job.id;
        project_detail.pylint_job.score = spawn_blocking(move || { get_code_quality(&pylint_project_id, &pylint_job_id) as f64}).await.unwrap();
        
        let bandit_project = project_detail.clone();
        let bandit_project_id = bandit_project.id;
        let bandit_job_id= bandit_project.bandit_job.id;
        project_detail.bandit_job.score = spawn_blocking(move || { get_security_sast_issues(&bandit_project_id, &bandit_job_id) as f64}).await.unwrap();
        
        let db_models = build_db_models(project_detail);
        write_to_influx(db_models).await;
    }

}

fn build_client() -> Result<reqwest::blocking::Client, reqwest::Error>{
    let client = reqwest::blocking::Client::builder()
        .user_agent("graphql-rust/0.10.0")
        .default_headers(
            std::iter::once((
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", "glpat-_N6xdZ_Tbra8M6uAXkET")).unwrap(),
            ))
            .collect())
        .build();

    return client;
}

fn build_db_models(project: ProjectDetailEntity) -> Vec<FloatScoreModel>{
    let mut models = Vec::new();
    let time = Utc::now();

    models.push(FloatScoreModel{
        time,
        project_id: project.clone().id,
        project_name: project.clone().name,
        metric: String::from("merge_request"),
        score: project.merge_requests as f64
    });
    
    models.push(FloatScoreModel{
        time,
        project_id: project.clone().id,
        project_name: project.clone().name,
        metric: String::from("code_quality"),
        score: project.pylint_job.score
    });
    
    models.push(FloatScoreModel{
        time,
        project_id: project.clone().id,
        project_name: project.clone().name,
        metric: String::from("security_sast"),
        score: project.bandit_job.score
    });
    
    models.push(FloatScoreModel{
        time,
        project_id: project.clone().id,
        project_name: project.clone().name,
        metric: String::from("vulnerable_dependencies"),
        score: project.safety_job.score
    });
    
    models.push(FloatScoreModel{
        time,
        project_id: project.id,
        project_name: project.name,
        metric: String::from("test_coverage"),
        score: project.test_job.coverage
    });
    
    models
}

async fn write_to_influx(models: Vec<FloatScoreModel>){
    // let client = Client::new("http://localhost:8086", "msc29").with_auth("msc29", "Ji3H4JN7QV4yzKPxIego6R9yGHwe5X");
    // // models.into_iter().for_each(|x| client.query(models[0].into_query(&models[0].metric)).await)
    // println!("{:#?}", client);
    // // models.into_iter().for_each(|m| client.query(m).await);
    // // assert_result_ok(&write_result);
    // // stream::unfold(models.into_iter(), |mut m| async {
    // //     let val = m.next()?;
    // //     let measurement = val.metric.clone();
    // //     let response = client.query(val.into_query(measurement)).await;
    // //     Some((response, m))
    // // });

    // // models.iter_mut().map(|item| tokio::spawn(client.query(val.into_query(measurement)).await));
    // // // now await them to get the resolve's to complete
    // // for task in tasks {
    // //     task.await.unwrap();
    // // }

    let mut headers = header::HeaderMap::new();
    headers.insert("Authorization", header::HeaderValue::from_str(&format!("Token {}", "OlraLqrJtquT4caC35bJheNgxEU8UmNpT3Sw5pI24Z6T6BlDDvud0ytvnbqa_ICJ7t3HeGxj-h0Oa2A0q98IAw==")).unwrap());
    headers.insert("Content-Type", header::HeaderValue::from_static("text/plain; charset=utf-8"));
    headers.insert("Accept", header::HeaderValue::from_static("application/json"));

    let client = reqwest::Client::builder()
    .user_agent("graphql-rust/0.10.0")
    .default_headers(
            // std::iter::once((
            //     reqwest::header::AUTHORIZATION,
            //     reqwest::header::HeaderValue::from_str(&format!("Token {}", "OlraLqrJtquT4caC35bJheNgxEU8UmNpT3Sw5pI24Z6T6BlDDvud0ytvnbqa_ICJ7t3HeGxj-h0Oa2A0q98IAw==")).unwrap(),
            // ))
            // .collect())
            headers)
            .build().unwrap();
            
    let mut stream = tokio_stream::iter(models);

    while let Some(score_model) = stream.next().await {
        let measurement = score_model.metric.clone();
        // client.query(score_model.into_query(measurement)).await.unwrap();
        let json = serde_json::to_string::<FloatScoreModel>(&score_model).unwrap();
        let res = client.post("http://localhost:8086/api/v2/write?org=MSC29&bucket=test&precision=ns").json(&json).send().await;
        match res {
            Ok(r) => println!("{:#?}", r),
            Err(e) => println!("{:#?}", e),
        }
    }
    
}

fn get_project_details_query(username: String) -> Response<get_project_details_query::ResponseData>{
    let variables_get_project_details_query = get_project_details_query::Variables {
        username
    };

    let request_body_get_project_details_query = GetProjectDetailsQuery::build_query(variables_get_project_details_query);

    let client = build_client().unwrap();
    let res = client.post("https://gitlab.com/api/graphql").json(&request_body_get_project_details_query).send();

    let response_body: Response<get_project_details_query::ResponseData> = res.unwrap().json().unwrap();
    // let project0 = .next();
    println!("{:#?}", response_body);

    response_body
}

fn get_code_quality(project_id: &str, job_id: &str) -> f32{   
    let client = build_client().unwrap();
    let url = format!("https://gitlab.com/api/v4/projects/{}/jobs/{}/trace", project_id, job_id);
    println!("{:#?}", url);
    let res = client.get(url).send();

    let response_body: String = res.unwrap().text().unwrap();
    // let project0 = .next();
    println!("{:#?}", response_body);
    
    let re = Regex::new(r"Your code has been rated at ([-0-9.]*)/10").unwrap();
    let cap = re.captures(&response_body).unwrap();

    println!("{:#?}", &cap[1]);

    cap[1].parse::<f32>().unwrap()
}

fn get_security_sast_issues(project_id: &str, job_id: &str) -> i32{   
    let client = build_client().unwrap();
    let url = format!("https://gitlab.com/api/v4/projects/{}/jobs/{}/trace", project_id, job_id);
    println!("{:#?}", url);
    let res = client.get(url).send();

    let response_body: String = res.unwrap().text().unwrap();
    // let project0 = .next();
    println!("{:#?}", response_body);
    
    let re_undefined = Regex::new(r"Undefined: (\d)").unwrap();
    let re_low = Regex::new(r"Low: (\d)").unwrap();
    let re_medium = Regex::new(r"Medium: (\d)").unwrap();
    let re_high = Regex::new(r"High: (\d)").unwrap();
    
    let cap_undefined = re_undefined.captures(&response_body).unwrap();
    let cap_low = re_low.captures(&response_body).unwrap();
    let cap_medium = re_medium.captures(&response_body).unwrap();
    let cap_high = re_high.captures(&response_body).unwrap();
    
    let score_undefined = i32::from_str_radix(&cap_undefined[1], 10).unwrap();
    let score_low = i32::from_str_radix(&cap_low[1], 10).unwrap();
    let score_medium = i32::from_str_radix(&cap_medium[1], 10).unwrap();
    let score_high = i32::from_str_radix(&cap_high[1], 10).unwrap();

    println!("{:#?}", score_undefined);
    println!("{:#?}", score_low);
    println!("{:#?}", score_medium);
    println!("{:#?}", score_high);

    score_undefined + score_low + score_medium + score_high
}

fn get_vulnerable_dependencies(project_id: &str, job_id: &str) -> i32 {
    let client = build_client().unwrap();
    let url = format!("https://gitlab.com/api/v4/projects/{}/jobs/{}/trace", project_id, job_id);
    let res = client.get(url).send();

    let response_body: String = res.unwrap().text().unwrap();
    // let project0 = .next();
    println!("{:#?}", response_body);
    
    let re = Regex::new(r"Safety found (\d) vulnerabilities").unwrap();
    let cap = re.captures(&response_body).unwrap();

    println!("{:#?}", &cap[1]);

    i32::from_str_radix(&cap[1], 10).unwrap()
}