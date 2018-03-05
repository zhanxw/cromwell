task quay {
    command {
        echo "hello"
    }
    runtime {
        docker: "quay.io/tjeandet/tj-ubuntu:latest"
    }
}

workflow docker_hash_quay {
    call quay 
}
