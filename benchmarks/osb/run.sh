for i in {1,}
do
echo $i
opensearch-benchmark execute-test --target-host ${HOSTS} \
     --workload-path ./query_workload.json  \
     --test-procedure ms-marco-query --pipeline benchmark-only  \
     --kill-running-processes \
     --on-error abort
done

# --client-options="basic_auth_user:'admin',basic_auth_password:'OpensearchTestBGE123.',use_ssl:true,verify_certs:false,timeout:30" \