import axios from "axios";

function createClient(url: string, token: string | null) {
  const client = axios.create({
    baseURL: url,
    timeout: 5000000,
    headers: {
      "Content-Type": "application/json",
    },
  });

  client.interceptors.request.use((config) => {
    config.headers.Authorization = `Bearer ${token ?? ""}`;
    return config;
  });

  client.interceptors.response.use(
    (response) => response,
    (error) => Promise.reject(error)
  );

  return client;
}

export class HttpClient {
  static async get<T>(url: string, token: string | null, params?: unknown) {
    const response = await createClient(url, token).get<T>(url, { params });
    return response.data;
  }

  static async post<T>(url: string, token: string | null, data: unknown) {
    const response = await createClient(url, token).post<T>(url, data);
    return response.data;
  }

  static async put<T>(url: string, token: string | null, data: unknown) {
    const response = await createClient(url, token).put<T>(url, data);
    return response.data;
  }

  static async delete<T>(url: string, token: string | null) {
    const response = await createClient(url, token).delete<T>(url);
    return response.data;
  }
}
